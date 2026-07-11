package platform

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the full environment contract (names match the repo .env).
type Config struct {
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string

	KafkaBootstrap    string
	SchemaRegistryURL string
	JWKSURL           string

	AccountGRPCAddr string
	FraudGRPCAddr   string
	MockPSPBaseURL  string

	HTTPPort int

	// Fraud timeout policy: below the limit a fraud outage fails open
	// (flag for review); at/above it the payment fails closed.
	FraudDeadline      time.Duration
	FraudFailOpenLimit int64

	FxQuoteTTL     time.Duration
	ResumeInterval time.Duration
	WalletTimeout  time.Duration

	// Step-up policy: a payment at or above the limit, or one made on a session
	// whose last authentication is older than StepUpMaxAge, demands a second
	// factor (amr must contain "mfa"). An old-but-valid token cannot move money.
	StepUpAmountLimit int64
	StepUpMaxAge      time.Duration

	// A payment stuck waiting for step-up, or unable to reach the gateway, is
	// failed once it passes these — nothing may wedge forever.
	RequiresActionTimeout time.Duration
	GatewaySubmitTimeout  time.Duration
	GatewayCaptureTimeout time.Duration

	// Settlement reconciliation: how often to compare our books against the
	// processor's report, and how long a capture may be settled-but-unbooked
	// before it counts as drift rather than a payment still in flight.
	ReconcileInterval time.Duration
	ReconcileGrace    time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		DBHost:             getenv("PAYMENT_DB_HOST", "payment-db"),
		DBPort:             getint("PAYMENT_DB_PORT", 5432),
		DBUser:             os.Getenv("PAYMENT_DB_USER"),
		DBPassword:         os.Getenv("PAYMENT_DB_PASSWORD"),
		DBName:             os.Getenv("PAYMENT_DB_NAME"),
		KafkaBootstrap:     getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
		SchemaRegistryURL:  getenv("SCHEMA_REGISTRY_URL", "http://apicurio-registry:8080/apis/ccompat/v7"),
		JWKSURL:            getenv("GATEWAY_JWKS_URL", "http://user-service:8080/.well-known/jwks.json"),
		AccountGRPCAddr:    getenv("ACCOUNT_GRPC_ADDR", "account-service:9090"),
		FraudGRPCAddr:      getenv("FRAUD_GRPC_ADDR", "fraud-service:9090"),
		MockPSPBaseURL:     getenv("MOCK_PSP_BASE_URL", "http://mock-psp:8080"),
		HTTPPort:           getint("HTTP_PORT", 8080),
		FraudDeadline:      getdur("FRAUD_DEADLINE", 150*time.Millisecond),
		FraudFailOpenLimit: getint64("FRAUD_FAIL_OPEN_LIMIT", 5000),
		FxQuoteTTL:         getdur("FX_QUOTE_TTL", 90*time.Second),
		ResumeInterval:     getdur("SAGA_RESUME_INTERVAL", 15*time.Second),
		WalletTimeout:      getdur("WALLET_RESULT_TIMEOUT", 10*time.Minute),

		StepUpAmountLimit:     getint64("STEPUP_AMOUNT_LIMIT", 100_000),
		StepUpMaxAge:          getdur("STEPUP_MAX_AGE", 15*time.Minute),
		RequiresActionTimeout: getdur("REQUIRES_ACTION_TIMEOUT", 15*time.Minute),
		GatewaySubmitTimeout:  getdur("GATEWAY_SUBMIT_TIMEOUT", 5*time.Minute),
		GatewayCaptureTimeout: getdur("GATEWAY_CAPTURE_TIMEOUT", 5*time.Minute),
		ReconcileInterval:     getdur("RECONCILE_INTERVAL", 60*time.Second),
		ReconcileGrace:        getdur("RECONCILE_GRACE", 60*time.Second),
	}
	if cfg.DBUser == "" || cfg.DBPassword == "" || cfg.DBName == "" {
		return cfg, fmt.Errorf("PAYMENT_DB_USER/PASSWORD/NAME are required")
	}
	return cfg, nil
}

func (c Config) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		c.DBUser, c.DBPassword, c.DBHost, c.DBPort, c.DBName)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getint(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getint64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func getdur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
