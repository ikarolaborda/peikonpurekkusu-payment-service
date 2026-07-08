// Package outbox implements the transactional outbox: rows written in the
// same transaction as the state change, published by a polling relay
// (FOR UPDATE SKIP LOCKED — replica-safe, at-least-once).
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/peikonpurekkusu/payment-service/internal/events"
)

// Writer records events inside the caller's transaction (ledger.OutboxWriter).
type Writer struct{}

func (Writer) Write(ctx context.Context, tx pgx.Tx, topic, aggregateType, aggregateID string, payload map[string]any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`insert into outbox (id, aggregatetype, aggregateid, type, payload) values ($1,$2,$3,$4,$5)`,
		events.UUIDv7(), aggregateType, aggregateID, topic, body)
	return err
}

// Relay polls unprocessed rows and publishes them framed to Kafka.
type Relay struct {
	pool     *pgxpool.Pool
	client   *kgo.Client
	registry *events.Registry
	log      *slog.Logger
	interval time.Duration
}

func NewRelay(pool *pgxpool.Pool, client *kgo.Client, registry *events.Registry, log *slog.Logger) *Relay {
	return &Relay{pool: pool, client: client, registry: registry, log: log, interval: 500 * time.Millisecond}
}

func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, err := r.drainOnce(ctx); err != nil {
				r.log.Error("outbox drain failed (will retry)", "error", err)
			} else if n > 0 {
				r.log.Debug("outbox drained", "published", n)
			}
		}
	}
}

func (r *Relay) drainOnce(ctx context.Context) (int, error) {
	published := 0
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`select id, aggregateid, type, payload, created_at from outbox
			  where processed_at is null order by id limit 50 for update skip locked`)
		if err != nil {
			return err
		}
		type row struct {
			id, aggregateID, topic string
			payload                []byte
			createdAt              time.Time
		}
		batch := []row{}
		for rows.Next() {
			var rr row
			if err := rows.Scan(&rr.id, &rr.aggregateID, &rr.topic, &rr.payload, &rr.createdAt); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, rr)
		}
		rows.Close()
		if len(batch) == 0 {
			return nil
		}

		ids := make([]string, 0, len(batch))
		for _, rr := range batch {
			var payload map[string]any
			if err := json.Unmarshal(rr.payload, &payload); err != nil {
				return fmt.Errorf("outbox row %s payload: %w", rr.id, err)
			}
			schemaID, err := r.registry.SchemaID(rr.topic)
			if err != nil {
				return err
			}
			value, err := events.Frame(schemaID, events.NewEnvelope(rr.id, rr.topic, rr.id, rr.createdAt, payload))
			if err != nil {
				return err
			}
			rec := &kgo.Record{Topic: rr.topic, Key: []byte(rr.aggregateID), Value: value}
			if err := r.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
				return fmt.Errorf("produce %s: %w", rr.topic, err)
			}
			ids = append(ids, rr.id)
		}
		if _, err := tx.Exec(ctx, `update outbox set processed_at = now() where id = any($1)`, ids); err != nil {
			return err
		}
		published = len(ids)
		return nil
	})
	return published, err
}
