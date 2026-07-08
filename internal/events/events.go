// Package events implements the platform event envelope and the Confluent
// wire format (magic byte 0x00 + big-endian int32 schema id + JSON) against
// the Apicurio ccompat endpoint. Schema ids come from
// GET /subjects/<topic>-value/versions/latest (cached); schemas themselves
// are registered by the schemas-init job from contracts/events/.
package events

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Envelope matches contracts/events/README.md.
type Envelope struct {
	EventID        string         `json:"event_id"`
	EventType      string         `json:"event_type"`
	SchemaVersion  int            `json:"schema_version"`
	OccurredAt     time.Time      `json:"occurred_at"`
	TenantID       string         `json:"tenant_id"`
	CorrelationID  string         `json:"correlation_id"`
	CausationID    *string        `json:"causation_id"`
	IdempotencyKey *string        `json:"idempotency_key"`
	Payload        map[string]any `json:"payload"`
}

// NewEnvelope builds an envelope; eventID is the outbox row id (uuidv7).
func NewEnvelope(eventID, topic, correlationID string, occurredAt time.Time, payload map[string]any) Envelope {
	return Envelope{
		EventID:       eventID,
		EventType:     topic,
		SchemaVersion: 1,
		OccurredAt:    occurredAt.UTC(),
		TenantID:      "peikon",
		CorrelationID: correlationID,
		Payload:       payload,
	}
}

// UUIDv7 returns a time-ordered UUID for outbox/event ids.
func UUIDv7() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}

// Registry resolves and caches schema ids per topic.
type Registry struct {
	baseURL string
	client  *http.Client
	mu      sync.RWMutex
	ids     map[string]int32
}

func NewRegistry(baseURL string) *Registry {
	return &Registry{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 10 * time.Second},
		ids:     map[string]int32{},
	}
}

func (r *Registry) SchemaID(topic string) (int32, error) {
	r.mu.RLock()
	if id, ok := r.ids[topic]; ok {
		r.mu.RUnlock()
		return id, nil
	}
	r.mu.RUnlock()

	resp, err := r.client.Get(fmt.Sprintf("%s/subjects/%s-value/versions/latest", r.baseURL, topic))
	if err != nil {
		return 0, fmt.Errorf("registry lookup for %s: %w", topic, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return 0, fmt.Errorf("registry lookup for %s: HTTP %d %s", topic, resp.StatusCode, body)
	}
	var out struct {
		ID int32 `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	r.mu.Lock()
	r.ids[topic] = out.ID
	r.mu.Unlock()
	return out.ID, nil
}

// Frame serializes an envelope in the Confluent wire format.
func Frame(schemaID int32, env Envelope) ([]byte, error) {
	payload, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 5, 5+len(payload))
	buf[0] = 0
	binary.BigEndian.PutUint32(buf[1:5], uint32(schemaID))
	return append(buf, payload...), nil
}

// Unframe parses a wire-format message back into an envelope. Messages
// without the magic byte are rejected (never mix raw JSON on these topics).
func Unframe(value []byte) (Envelope, error) {
	var env Envelope
	if len(value) < 6 || value[0] != 0 {
		return env, fmt.Errorf("not a confluent-framed message")
	}
	if err := json.Unmarshal(value[5:], &env); err != nil {
		return env, fmt.Errorf("envelope parse: %w", err)
	}
	if env.EventID == "" || env.EventType == "" {
		return env, fmt.Errorf("envelope missing event_id/event_type")
	}
	return env, nil
}
