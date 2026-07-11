// Package consumer resumes wallet sagas when the asynchronous processor
// reports its outcome via gateway.psp.completed.v1. Idempotent through
// processed_events; poison messages go to the per-group DLQ.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/peikonpurekkusu/payment-service/internal/events"
	"github.com/peikonpurekkusu/payment-service/internal/saga"
)

const (
	group         = "payment-service"
	topicComplete = "gateway.psp.completed.v1"
	maxAttempts   = 3
)

type Consumer struct {
	pool      *pgxpool.Pool
	engine    *saga.Engine
	client    *kgo.Client
	producer  *kgo.Client
	validator *events.Validator
	log       *slog.Logger
}

func New(pool *pgxpool.Pool, engine *saga.Engine, bootstrap []string, producer *kgo.Client,
	validator *events.Validator, log *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topicComplete),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{pool: pool, engine: engine, client: client, producer: producer, validator: validator, log: log}, nil
}

func (c *Consumer) Close() { c.client.Close() }

func (c *Consumer) Run(ctx context.Context) {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				c.log.Error("kafka fetch error", "topic", e.Topic, "error", e.Err)
			}
			time.Sleep(time.Second)
			continue
		}
		settled := true
		fetches.EachRecord(func(rec *kgo.Record) {
			if !c.handleWithRetry(ctx, rec) {
				settled = false
			}
		})
		// A record is "settled" only once it is either processed or safely in the
		// DLQ. Committing an unsettled batch would advance past an event we never
		// stored anywhere — silent loss. Leave the offsets and let it redeliver;
		// handling is idempotent, so reprocessing the whole batch is safe.
		if !settled {
			c.log.Error("batch not settled — offsets left uncommitted for redelivery")
			time.Sleep(time.Second)
			continue
		}
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			c.log.Error("offset commit failed", "error", err)
		}
	}
}

// handleWithRetry reports whether the record is settled (processed, or durably
// dead-lettered). False means the offset must not advance.
func (c *Consumer) handleWithRetry(ctx context.Context, rec *kgo.Record) bool {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := c.handle(ctx, rec)
		if err == nil {
			return true
		}
		if poison := (*poisonError)(nil); errors.As(err, &poison) {
			return c.deadLetter(ctx, rec, poison.cause)
		}
		lastErr = err
		c.log.Warn("event handling failed", "topic", rec.Topic, "attempt", attempt, "error", err)
		time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
	}
	return c.deadLetter(ctx, rec, lastErr)
}

func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	// Validate against the exact schema the producer framed with, BEFORE any
	// field is read. Registry outage blocks HERE: franz-go commits per batch,
	// so returning "unsettled" would let the next successful batch commit past
	// this record — blocking inside the handler is what holds the offset.
	for {
		err := c.validator.Validate(ctx, rec.Value)
		if err == nil {
			break
		}
		if errors.Is(err, events.ErrRegistryUnavailable) {
			c.log.Warn("schema registry unavailable — holding", "topic", rec.Topic, "offset", rec.Offset)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
			continue
		}
		return poison(err)
	}

	env, err := events.Unframe(rec.Value)
	if err != nil {
		return poison(err)
	}
	eventID, err := uuid.Parse(env.EventID)
	if err != nil {
		return poison(fmt.Errorf("bad event_id: %w", err))
	}
	paymentID, err := uuid.Parse(str(env.Payload["payment_id"]))
	if err != nil {
		return poison(fmt.Errorf("payment_id: %w", err))
	}

	var seen bool
	if err := c.pool.QueryRow(ctx,
		`select exists (select 1 from processed_events where event_id = $1)`, eventID).Scan(&seen); err != nil {
		return err
	}
	if seen {
		return nil
	}

	// Validation guarantees outcome (schema-required); the check is the
	// backstop that keeps a bypass from reading "" and calling it a decline.
	outcome := str(env.Payload["outcome"])
	if outcome == "" {
		return poison(fmt.Errorf("payload missing 'outcome'"))
	}

	// Effect first, mark second. CompleteFromGateway is idempotent (it re-loads
	// the payment and only acts on step='submitted_to_gateway'), so a crash
	// before the mark redelivers harmlessly. Marking first would strand the
	// payment: the redelivery would be skipped and the hold would never resolve.
	if err := c.engine.CompleteFromGateway(ctx, paymentID,
		outcome == "approved", str(env.Payload["psp_reference"]), str(env.Payload["decline_code"])); err != nil {
		return err
	}

	_, err = c.pool.Exec(ctx,
		`insert into processed_events (event_id) values ($1) on conflict do nothing`, eventID)
	return err
}

// deadLetter reports whether the record reached the DLQ. A failed DLQ publish
// must not settle the record — dropping it here is the silent loss the DLQ exists to prevent.
func (c *Consumer) deadLetter(ctx context.Context, rec *kgo.Record, cause error) bool {
	dlq := fmt.Sprintf("%s.%s.dlq", group, rec.Topic)
	headers := append(rec.Headers,
		kgo.RecordHeader{Key: "x-exception", Value: []byte(fmt.Sprint(cause))},
		kgo.RecordHeader{Key: "x-original-topic", Value: []byte(rec.Topic)},
		kgo.RecordHeader{Key: "x-original-partition", Value: []byte(fmt.Sprint(rec.Partition))},
		kgo.RecordHeader{Key: "x-original-offset", Value: []byte(fmt.Sprint(rec.Offset))},
		kgo.RecordHeader{Key: "x-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		kgo.RecordHeader{Key: "x-consumer-group", Value: []byte(group)},
	)
	if err := c.producer.ProduceSync(ctx, &kgo.Record{
		Topic: dlq, Key: rec.Key, Value: rec.Value, Headers: headers,
	}).FirstErr(); err != nil {
		c.log.Error("DLQ publish failed — offset held for redelivery", "dlq", dlq, "cause", cause, "error", err)
		return false
	}
	c.log.Warn("message dead-lettered", "dlq", dlq, "cause", cause)
	return true
}

// poisonError marks an event that can never succeed on redelivery — it goes
// straight to the DLQ instead of burning the retry budget.
type poisonError struct{ cause error }

func (p *poisonError) Error() string { return p.cause.Error() }

func poison(cause error) error { return &poisonError{cause: cause} }

func str(v any) string {
	s, _ := v.(string)
	return s
}
