# payment-service

The saga orchestrator: drives every payment through fraud screening → funds
hold → gateway submission → capture, with persisted transitions, deterministic
idempotency and compensation. Go 1.26.

## The saga (State pattern, orchestrated)

```
step:   requested → fraud_screened → funds_held → submitted_to_gateway → captured → done
status: processing → succeeded | failed | requires_action        (PaymentIntent-style)
```

- Every transition is persisted with its outbox event in one transaction; a
  crash resumes exactly where it stopped (`ResumeStale` sweeper, 15 s).
- Side effects are idempotent by construction: account-service request ids are
  derived from the payment id (`hold:<id>`, `capture:<id>`, `release:<id>`),
  the PSP is idempotent by reference.
- **Compensation:** any failure after the hold releases it via gRPC; the
  emitted `payments.payment.failed.v1` also lets account-service release
  orphaned holds event-driven (belt and braces).
- Wallet payments pause at `submitted_to_gateway` and resume when
  `gateway.psp.completed.v1` arrives (async processor flow).

## Interfaces

- `POST /payments` — **Idempotency-Key required** (Stripe model: replay same
  key+payload → stored response with `Idempotency-Replayed: true`; different
  payload → 422; in-flight → 409; responses recorded even for failures; 24 h TTL).
- `GET /payments/{id}` · `GET /payments/{id}/events` (SSE status stream) ·
  `POST /payments/fx-quote` (rate locked until expiry; expired quote → 400) ·
  `GET /payments/instruments` · `GET /payments/merchants`.
- Consumes `gateway.psp.completed.v1`; emits `payments.payment.{requested,authorized,captured,failed}.v1`.

## Fraud timeout policy (Strategy — lives with the caller)

`FRAUD_DEADLINE` (150 ms) gRPC deadline; on outage: amount < `FRAUD_FAIL_OPEN_LIMIT`
(5000 minor units) proceeds flagged (`risk_score = -1`), at/above fails closed
(`gateway_unavailable`). DENY → `fraud_denied`; STEP_UP/HOLD → `requires_action`.

## Patterns map

- **Facade** — httpapi over engine/idempotency/quotes
- **State** — saga engine transitions
- **Strategy + Adapter + Factory** — `internal/psp`: PaymentProcessor port,
  card/wallet adapters, factory by method
- **Circuit Breaker + Retry** — gobreaker + jittered backoff decorator around every adapter
- **Idempotency-Key** — `internal/idempotency` (Stripe-style records)
- **Transactional Outbox** — shared relay pattern (`FOR UPDATE SKIP LOCKED`)

## mock-psp (services/mock-psp)

Deterministic external gateway: amounts ending **42** → hard decline, ending
**13** → transient 503 band (exercises retry/breaker), `wallet` → async result
via Kafka (ending **43** → async decline); idempotent by reference.
