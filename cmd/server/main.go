// payment-service — orchestrated payment saga. HTTP :8080.
package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver for goose
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
	"github.com/twmb/franz-go/pkg/kgo"

	payment "github.com/peikonpurekkusu/payment-service"
	"github.com/peikonpurekkusu/payment-service/internal/accountclient"
	"github.com/peikonpurekkusu/payment-service/internal/consumer"
	"github.com/peikonpurekkusu/payment-service/internal/events"
	"github.com/peikonpurekkusu/payment-service/internal/fraudclient"
	"github.com/peikonpurekkusu/payment-service/internal/gatewayauth"
	"github.com/peikonpurekkusu/payment-service/internal/httpapi"
	"github.com/peikonpurekkusu/payment-service/internal/idempotency"
	"github.com/peikonpurekkusu/payment-service/internal/outbox"
	"github.com/peikonpurekkusu/payment-service/internal/platform"
	"github.com/peikonpurekkusu/payment-service/internal/psp"
	"github.com/peikonpurekkusu/payment-service/internal/pubsub"
	"github.com/peikonpurekkusu/payment-service/internal/saga"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(selfProbe())
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := platform.Load()
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, cfg.DSN())
	if err != nil {
		return fmt.Errorf("pgx pool: %w", err)
	}
	defer pool.Close()
	if err := migrate(ctx, cfg); err != nil {
		return fmt.Errorf("migrations: %w", err)
	}
	log.Info("migrations applied")

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(splitCSV(cfg.KafkaBootstrap)...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer producer.Close()

	registry := events.NewRegistry(cfg.SchemaRegistryURL)
	relay := outbox.NewRelay(pool, producer, registry, log)
	go relay.Run(ctx)

	fraud, err := fraudclient.New(cfg.FraudGRPCAddr, cfg.FraudDeadline, cfg.FraudFailOpenLimit, log)
	if err != nil {
		return fmt.Errorf("fraud client: %w", err)
	}
	accounts, err := accountclient.New(cfg.AccountGRPCAddr)
	if err != nil {
		return fmt.Errorf("account client: %w", err)
	}

	bus := pubsub.New()
	engine := saga.NewEngine(pool, outbox.Writer{}, fraud, accounts, psp.NewFactory(cfg.MockPSPBaseURL), bus, log)

	validator := events.NewValidator(cfg.SchemaRegistryURL)
	cons, err := consumer.New(pool, engine, splitCSV(cfg.KafkaBootstrap), producer, validator, log)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	defer cons.Close()
	go cons.Run(ctx)

	idem := idempotency.NewStore(pool)
	go every(ctx, cfg.ResumeInterval, func() { engine.ResumeStale(ctx, 30*time.Second) })
	go every(ctx, cfg.ResumeInterval, func() { engine.ExpireStaleWallets(ctx, cfg.WalletTimeout) })
	go every(ctx, cfg.ResumeInterval, func() { engine.ExpireStaleRequiresAction(ctx, cfg.RequiresActionTimeout) })
	go every(ctx, cfg.ResumeInterval, func() { engine.ExpireStuckSubmissions(ctx, cfg.GatewaySubmitTimeout) })
	go every(ctx, cfg.ResumeInterval, func() { engine.ExpireStuckCaptures(ctx, cfg.GatewayCaptureTimeout) })
	go every(ctx, time.Minute, func() { engine.AlertStalledLedgerCaptures(ctx, 2*time.Minute) })
	reconciler := saga.NewReconciler(engine, cfg.MockPSPBaseURL, cfg.ReconcileGrace)
	go every(ctx, cfg.ReconcileInterval, func() { reconciler.Run(ctx) })
	go func() { reconciler.Run(ctx) }() // first pass immediately, not a minute after boot
	go every(ctx, time.Hour, func() {
		if n, err := idem.Purge(ctx); err == nil && n > 0 {
			log.Info("idempotency keys purged", "count", n)
		}
	})

	kafkaOK := func() bool {
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return producer.Ping(pingCtx) == nil
	}
	verifier, err := gatewayauth.New(ctx, cfg.JWKSURL, log)
	if err != nil {
		return fmt.Errorf("gateway auth verifier: %w", err)
	}
	httpSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:           verifier.Middleware(httpapi.New(pool, idem, engine, bus, cfg.FxQuoteTTL, cfg.StepUpAmountLimit, cfg.StepUpMaxAge, kafkaOK, log).WithReconciler(reconciler).Handler()),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      0, // SSE endpoints stream indefinitely
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Info("HTTP listening", "port", cfg.HTTPPort)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http serve", "error", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shCtx)
	return nil
}

func migrate(ctx context.Context, cfg platform.Config) error {
	sqlDB, err := goose.OpenDBWithDriver("pgx", cfg.DSN())
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return err
	}
	migrations, err := fs.Sub(payment.MigrationsFS, "migrations")
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, migrations,
		goose.WithSessionLocker(locker))
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}

func selfProbe() int {
	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8080"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://localhost:" + port + "/health/ready")
	if err != nil || resp.StatusCode != http.StatusOK {
		return 1
	}
	resp.Body.Close()
	return 0
}

func every(ctx context.Context, interval time.Duration, fn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn()
		}
	}
}

func splitCSV(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}
