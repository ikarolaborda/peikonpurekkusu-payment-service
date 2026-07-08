package idempotency

import "testing"

func TestHashBodyIsStableAndSensitive(t *testing.T) {
	a := HashBody([]byte(`{"amount":100,"merchant":"m-books"}`))
	b := HashBody([]byte(`{"amount":100,"merchant":"m-books"}`))
	c := HashBody([]byte(`{"amount":101,"merchant":"m-books"}`))
	if a != b {
		t.Fatal("identical payloads must hash identically")
	}
	if a == c {
		t.Fatal("different payloads must hash differently")
	}
	if len(a) != 64 {
		t.Fatalf("expected 64-char sha256 hex, got %d", len(a))
	}
}
