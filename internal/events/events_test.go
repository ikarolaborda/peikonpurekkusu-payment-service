package events

import (
	"testing"
	"time"
)

func TestFrameUnframeRoundtrip(t *testing.T) {
	env := NewEnvelope(UUIDv7(), "accounts.funds.held.v1", "corr-1", time.Now(), map[string]any{
		"hold_id":            "h1",
		"amount_minor_units": float64(500),
	})
	framed, err := Frame(42, env)
	if err != nil {
		t.Fatal(err)
	}
	if framed[0] != 0 {
		t.Fatalf("magic byte missing")
	}
	got, err := Unframe(framed)
	if err != nil {
		t.Fatal(err)
	}
	if got.EventType != env.EventType || got.EventID != env.EventID {
		t.Fatalf("roundtrip mismatch: %+v vs %+v", got, env)
	}
}

func TestUnframeRejectsRawJSON(t *testing.T) {
	if _, err := Unframe([]byte(`{"event_id":"x"}`)); err == nil {
		t.Fatal("raw JSON must be rejected — never mix framed and unframed messages")
	}
}

func TestUUIDv7IsTimeOrdered(t *testing.T) {
	a := UUIDv7()
	time.Sleep(2 * time.Millisecond)
	b := UUIDv7()
	if !(a < b) {
		t.Fatalf("uuidv7 not monotonic: %s !< %s", a, b)
	}
}
