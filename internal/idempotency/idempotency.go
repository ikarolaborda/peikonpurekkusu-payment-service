// Package idempotency implements Stripe-style idempotency records for the
// payment initiation endpoint: same key + same payload replays the stored
// response; same key + different payload is a hard error; a key currently
// executing returns 409. Responses are recorded once execution begins.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrPayloadMismatch = errors.New("idempotency key reused with a different payload")
	ErrInFlight        = errors.New("request with this idempotency key is in flight")
)

const keyTTL = 24 * time.Hour

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Replay is a previously stored response.
type Replay struct {
	Code int
	Body json.RawMessage
}

func HashBody(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// Begin claims the key. Returns (nil, nil) when this request should execute;
// a Replay when a completed response exists; an error for mismatch/in-flight.
func (s *Store) Begin(ctx context.Context, userID uuid.UUID, key, requestHash string) (*Replay, error) {
	tag, err := s.pool.Exec(ctx, `
		insert into idempotency_keys (key, user_id, request_hash, locked_at)
		values ($1, $2, $3, now())
		on conflict (user_id, key) do nothing`, key, userID, requestHash)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 1 {
		return nil, nil // fresh claim — caller executes
	}

	var storedHash string
	var code *int
	var body []byte
	var createdAt time.Time
	err = s.pool.QueryRow(ctx, `
		select request_hash, response_code, response_body, created_at
		  from idempotency_keys where user_id=$1 and key=$2`, userID, key).
		Scan(&storedHash, &code, &body, &createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrInFlight // raced a concurrent purge; treat as in flight
	}
	if err != nil {
		return nil, err
	}
	if storedHash != requestHash {
		return nil, ErrPayloadMismatch
	}
	if code == nil {
		return nil, ErrInFlight
	}
	return &Replay{Code: *code, Body: body}, nil
}

// Complete stores the response for future replays (including failures — a
// replayed 4xx/5xx must return the original outcome, per the Stripe model).
func (s *Store) Complete(ctx context.Context, userID uuid.UUID, key string, code int, body []byte) error {
	_, err := s.pool.Exec(ctx, `
		update idempotency_keys
		   set response_code=$3, response_body=$4, locked_at=null
		 where user_id=$1 and key=$2`, userID, key, code, body)
	return err
}

// Purge drops expired keys (client retry windows must fit inside the TTL).
func (s *Store) Purge(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`delete from idempotency_keys where created_at < now() - $1::interval`, keyTTL.String())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
