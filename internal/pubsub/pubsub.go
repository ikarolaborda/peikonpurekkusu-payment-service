// Package pubsub is the in-process status broadcast feeding the SSE endpoint.
// Single-replica scope by design; multi-replica deployments swap this for
// Postgres LISTEN/NOTIFY or Redis pub/sub behind the same interface.
package pubsub

import "sync"

type Update struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
	Step      string `json:"step"`
}

type Bus struct {
	mu   sync.RWMutex
	subs map[string]map[chan Update]struct{}
}

func New() *Bus {
	return &Bus{subs: map[string]map[chan Update]struct{}{}}
}

func (b *Bus) Subscribe(paymentID string) (<-chan Update, func()) {
	ch := make(chan Update, 8)
	b.mu.Lock()
	if b.subs[paymentID] == nil {
		b.subs[paymentID] = map[chan Update]struct{}{}
	}
	b.subs[paymentID][ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.subs[paymentID], ch)
		if len(b.subs[paymentID]) == 0 {
			delete(b.subs, paymentID)
		}
		b.mu.Unlock()
		close(ch)
	}
}

func (b *Bus) Publish(u Update) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.subs[u.PaymentID] {
		select {
		case ch <- u: // slow subscribers drop updates rather than block the saga
		default:
		}
	}
}
