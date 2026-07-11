// Package psp is the external-processor integration layer:
//
//	PaymentProcessor (port) ← CardProcessor / WalletProcessor (adapters)
//	                           selected by ProcessorFactory (factory)
//	                           wrapped in retry + circuit breaker
//
// Adapters normalize each provider's shape to one contract; the saga never
// sees provider specifics. Calls are idempotent by our payment id, so
// retries and saga resumes are safe.
package psp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/sony/gobreaker/v2"
)

// Outcome of an authorization attempt.
type Outcome string

const (
	OutcomeApproved Outcome = "approved"
	OutcomeDeclined Outcome = "declined"
	OutcomePending  Outcome = "pending" // async processors deliver the result via Kafka
)

// AuthResult is the normalized processor response.
type AuthResult struct {
	Outcome      Outcome
	PSPReference string
	DeclineCode  string
}

// ErrUnavailable marks transient processor trouble (retried, then breaker).
var ErrUnavailable = errors.New("processor unavailable")

// PaymentProcessor is the port the saga depends on.
type PaymentProcessor interface {
	Authorize(ctx context.Context, paymentID string, amountMinor int64, currency, token string) (AuthResult, error)
	Capture(ctx context.Context, pspReference string) error
	Reverse(ctx context.Context, pspReference string) error
	Name() string
}

// Factory selects the adapter for a payment method (Strategy + Factory).
type Factory struct {
	processors map[string]PaymentProcessor
}

func NewFactory(baseURL string) *Factory {
	client := &http.Client{Timeout: 5 * time.Second}
	return &Factory{processors: map[string]PaymentProcessor{
		"card":   withResilience(&cardProcessor{baseURL: baseURL, http: client}),
		"wallet": withResilience(&walletProcessor{baseURL: baseURL, http: client}),
	}}
}

func (f *Factory) For(method string) (PaymentProcessor, error) {
	p, ok := f.processors[method]
	if !ok {
		return nil, fmt.Errorf("unsupported payment method %q", method)
	}
	return p, nil
}

// ── resilience decorator: bounded retries w/ jittered backoff + breaker ─────

type resilientProcessor struct {
	inner   PaymentProcessor
	breaker *gobreaker.CircuitBreaker[AuthResult]
	plain   *gobreaker.CircuitBreaker[struct{}]
}

func withResilience(inner PaymentProcessor) PaymentProcessor {
	settings := gobreaker.Settings{
		Name:        inner.Name(),
		MaxRequests: 2,
		Interval:    30 * time.Second,
		Timeout:     10 * time.Second, // open → half-open
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.Requests >= 5 && float64(c.TotalFailures)/float64(c.Requests) > 0.5
		},
	}
	return &resilientProcessor{
		inner:   inner,
		breaker: gobreaker.NewCircuitBreaker[AuthResult](settings),
		plain:   gobreaker.NewCircuitBreaker[struct{}](settings),
	}
}

func (r *resilientProcessor) Name() string { return r.inner.Name() }

// asUnavailable maps the breaker's own refusals onto ErrUnavailable. Without
// this they surface as unclassified errors, and the saga reads an unclassified
// error as a DECLINE — so an open circuit (i.e. the processor is down) would
// tell the customer their card was refused.
func asUnavailable(err error) error {
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return fmt.Errorf("%w: circuit open", ErrUnavailable)
	}
	return err
}

func retry[T any](ctx context.Context, attempts int, fn func() (T, error)) (T, error) {
	var out T
	var err error
	for i := 0; i < attempts; i++ {
		out, err = fn()
		if err == nil || !errors.Is(err, ErrUnavailable) {
			return out, err // success, or a hard (non-retryable) failure
		}
		backoff := time.Duration(1<<i)*200*time.Millisecond + time.Duration(rand.IntN(150))*time.Millisecond
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-time.After(backoff):
		}
	}
	return out, err
}

func (r *resilientProcessor) Authorize(ctx context.Context, paymentID string, amount int64, currency, token string) (AuthResult, error) {
	return retry(ctx, 3, func() (AuthResult, error) {
		out, err := r.breaker.Execute(func() (AuthResult, error) {
			return r.inner.Authorize(ctx, paymentID, amount, currency, token)
		})
		return out, asUnavailable(err)
	})
}

func (r *resilientProcessor) Capture(ctx context.Context, ref string) error {
	_, err := retry(ctx, 3, func() (struct{}, error) {
		out, err := r.plain.Execute(func() (struct{}, error) {
			return struct{}{}, r.inner.Capture(ctx, ref)
		})
		return out, asUnavailable(err)
	})
	return err
}

func (r *resilientProcessor) Reverse(ctx context.Context, ref string) error {
	_, err := retry(ctx, 3, func() (struct{}, error) {
		out, err := r.plain.Execute(func() (struct{}, error) {
			return struct{}{}, r.inner.Reverse(ctx, ref)
		})
		return out, asUnavailable(err)
	})
	return err
}

// ── card adapter (synchronous authorize/capture, Visa-style) ────────────────

type cardProcessor struct {
	baseURL string
	http    *http.Client
}

func (c *cardProcessor) Name() string { return "mockpay-card" }

type authorizeReq struct {
	Reference   string `json:"reference"`
	AmountMinor int64  `json:"amount_minor_units"`
	Currency    string `json:"currency_code"`
	Method      string `json:"method"`
	Token       string `json:"token"`
}

type authorizeResp struct {
	PSPReference string `json:"psp_reference"`
	Outcome      string `json:"outcome"`
	DeclineCode  string `json:"decline_code"`
	Pending      bool   `json:"pending"`
}

func (c *cardProcessor) Authorize(ctx context.Context, paymentID string, amount int64, currency, token string) (AuthResult, error) {
	return doAuthorize(ctx, c.http, c.baseURL, authorizeReq{
		Reference: paymentID, AmountMinor: amount, Currency: currency, Method: "card", Token: token,
	})
}

func (c *cardProcessor) Capture(ctx context.Context, ref string) error {
	return doPost(ctx, c.http, c.baseURL+"/capture/"+ref)
}

func (c *cardProcessor) Reverse(ctx context.Context, ref string) error {
	return doPost(ctx, c.http, c.baseURL+"/reverse/"+ref)
}

// ── wallet adapter (asynchronous, PayPal-style: result arrives via Kafka) ───

type walletProcessor struct {
	baseURL string
	http    *http.Client
}

func (w *walletProcessor) Name() string { return "mockpay-wallet" }

func (w *walletProcessor) Authorize(ctx context.Context, paymentID string, amount int64, currency, token string) (AuthResult, error) {
	res, err := doAuthorize(ctx, w.http, w.baseURL, authorizeReq{
		Reference: paymentID, AmountMinor: amount, Currency: currency, Method: "wallet", Token: token,
	})
	if err != nil {
		return res, err
	}
	// wallet flow acknowledges and completes later via gateway.psp.completed.v1
	if res.Outcome == "" || res.Outcome == OutcomePending {
		res.Outcome = OutcomePending
	}
	return res, nil
}

func (w *walletProcessor) Capture(ctx context.Context, ref string) error {
	return doPost(ctx, w.http, w.baseURL+"/capture/"+ref)
}

func (w *walletProcessor) Reverse(ctx context.Context, ref string) error {
	return doPost(ctx, w.http, w.baseURL+"/reverse/"+ref)
}

// ── shared HTTP plumbing ─────────────────────────────────────────────────────

func doAuthorize(ctx context.Context, client *http.Client, baseURL string, body authorizeReq) (AuthResult, error) {
	payload, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/authorize", bytes.NewReader(payload))
	if err != nil {
		return AuthResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return AuthResult{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return AuthResult{}, fmt.Errorf("%w: HTTP %d", ErrUnavailable, resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return AuthResult{}, fmt.Errorf("processor rejected request: HTTP %d %s", resp.StatusCode, b)
	}
	var out authorizeResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return AuthResult{}, fmt.Errorf("%w: bad response: %v", ErrUnavailable, err)
	}
	res := AuthResult{PSPReference: out.PSPReference, DeclineCode: out.DeclineCode}
	switch {
	case out.Pending:
		res.Outcome = OutcomePending
	case out.Outcome == "approved":
		res.Outcome = OutcomeApproved
	default:
		res.Outcome = OutcomeDeclined
	}
	return res, nil
}

func doPost(ctx context.Context, client *http.Client, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("%w: HTTP %d", ErrUnavailable, resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("processor rejected: HTTP %d", resp.StatusCode)
	}
	return nil
}
