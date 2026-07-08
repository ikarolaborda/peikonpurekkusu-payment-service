// Package consumer resumes wallet sagas when the asynchronous processor
// reports its outcome via gateway.psp.completed.v1. Idempotent through
// processed_events; poison messages go to the per-group DLQ.
package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	pool     *pgxpool.Pool
	engine   *saga.Engine
	client   *kgo.Client
	producer *kgo.Client
	log      *slog.Logger
}

func New(pool *pgxpool.Pool, engine *saga.Engine, bootstrap []string, producer *kgo.Client, log *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topicComplete),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{pool: pool, engine: engine, client: client, producer: producer, log: log}, nil
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
		fetches.EachRecord(func(rec *kgo.Record) {
			c.handleWithRetry(ctx, rec)
		})
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			c.log.Error("offset commit failed", "error", err)
		}
	}
}

func (c *Consumer) handleWithRetry(ctx context.Context, rec *kgo.Record) {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := c.handle(ctx, rec); err != nil {
			lastErr = err
			c.log.Warn("event handling failed", "topic", rec.Topic, "attempt", attempt, "error", err)
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
			continue
		}
		return
	}
	c.deadLetter(ctx, rec, lastErr)
}

func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	env, err := events.Unframe(rec.Value)
	if err != nil {
		c.deadLetter(ctx, rec, err)
		return nil
	}
	eventID, err := uuid.Parse(env.EventID)
	if err != nil {
		c.deadLetter(ctx, rec, fmt.Errorf("bad event_id: %w", err))
		return nil
	}

	var fresh bool
	err = pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`insert into processed_events (event_id) values ($1) on conflict do nothing`, eventID)
		if err != nil {
			return err
		}
		fresh = tag.RowsAffected() == 1
		return nil
	})
	if err != nil || !fresh {
		return err
	}

	paymentID, err := uuid.Parse(str(env.Payload["payment_id"]))
	if err != nil {
		c.deadLetter(ctx, rec, fmt.Errorf("payment_id: %w", err))
		return nil
	}
	outcome := str(env.Payload["outcome"])
	c.engine.CompleteFromGateway(ctx, paymentID,
		outcome == "approved", str(env.Payload["psp_reference"]), str(env.Payload["decline_code"]))
	return nil
}

func (c *Consumer) deadLetter(ctx context.Context, rec *kgo.Record, cause error) {
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
		c.log.Error("DLQ publish FAILED — message dropped", "dlq", dlq, "cause", cause, "error", err)
		return
	}
	c.log.Warn("message dead-lettered", "dlq", dlq, "cause", cause)
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
