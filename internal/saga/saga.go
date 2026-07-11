// Package saga orchestrates the payment lifecycle (State pattern, orchestrated
// saga): one owner drives fraud screening → funds hold → gateway submission →
// capture, persisting every transition (with its outbox event, same tx) so a
// crash resumes exactly where it stopped. Compensation = release the hold and
// emit payments.payment.failed.v1.
//
//	step:   requested → fraud_screened → funds_held → submitted_to_gateway → captured → done
//	status: processing → succeeded | failed | requires_action
package saga

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	fraudv1 "github.com/ikarolaborda/peikonpurekkusu-contracts/gen/go/fraud/v1"
	"github.com/peikonpurekkusu/payment-service/internal/accountclient"
	"github.com/peikonpurekkusu/payment-service/internal/fraudclient"
	"github.com/peikonpurekkusu/payment-service/internal/psp"
	"github.com/peikonpurekkusu/payment-service/internal/pubsub"
)

// Payment is the saga aggregate as loaded from the payments table.
type Payment struct {
	ID           uuid.UUID
	UserID       uuid.UUID
	AccountID    uuid.UUID
	MerchantID   string
	InstrumentID uuid.UUID
	Method       string // card | wallet (joined from instruments)
	Token        string
	Amount       int64
	Currency     string
	FxQuoteID    *uuid.UUID
	Status       string
	Step         string
	HoldID       *uuid.UUID
	PSPReference *string
	RiskScore    *int32
}

// OutboxWriter matches the shared outbox writer contract.
type OutboxWriter interface {
	Write(ctx context.Context, tx pgx.Tx, topic, aggregateType, aggregateID string, payload map[string]any) error
}

type Engine struct {
	pool     *pgxpool.Pool
	outbox   OutboxWriter
	fraud    *fraudclient.Client
	accounts *accountclient.Client
	psps     *psp.Factory
	bus      *pubsub.Bus
	log      *slog.Logger
}

func NewEngine(pool *pgxpool.Pool, outbox OutboxWriter, fraud *fraudclient.Client,
	accounts *accountclient.Client, psps *psp.Factory, bus *pubsub.Bus, log *slog.Logger) *Engine {
	return &Engine{pool: pool, outbox: outbox, fraud: fraud, accounts: accounts, psps: psps, bus: bus, log: log}
}

// Advance drives the saga from its current step to a terminal or waiting
// state. Idempotent: every side effect keys on the payment id.
func (e *Engine) Advance(ctx context.Context, paymentID uuid.UUID) {
	for {
		p, err := e.load(ctx, paymentID)
		if err != nil {
			e.log.Error("saga load failed", "payment_id", paymentID, "error", err)
			return
		}
		if p.Status != "processing" {
			return // terminal or requires_action
		}

		var next bool
		switch p.Step {
		case "requested":
			next = e.stepFraud(ctx, p)
		case "fraud_screened":
			next = e.stepHold(ctx, p)
		case "funds_held":
			next = e.stepSubmit(ctx, p)
		case "submitted_to_gateway":
			// Card payments continue inline (authorization already resolved);
			// wallet payments wait here for gateway.psp.completed.v1.
			if p.PSPReference == nil {
				return
			}
			next = e.stepPSPCapture(ctx, p)
		case "psp_captured":
			next = e.stepLedgerCapture(ctx, p)
		default:
			return
		}
		if !next {
			return
		}
	}
}

// ResumeStale re-drives sagas that lost their goroutine (crash/restart).
func (e *Engine) ResumeStale(ctx context.Context, olderThan time.Duration) {
	rows, err := e.pool.Query(ctx,
		`select id from payments
		  where status = 'processing' and updated_at < now() - $1::interval
		  limit 20`, olderThan.String())
	if err != nil {
		e.log.Error("resume scan failed", "error", err)
		return
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	for _, id := range ids {
		e.log.Info("resuming stale saga", "payment_id", id)
		go e.Advance(context.WithoutCancel(ctx), id)
	}
}

// CompleteFromGateway is called by the Kafka consumer when an async processor
// (wallet) reports its outcome. It returns an error when the outcome could not
// be durably applied, so the caller can redeliver rather than mark the event
// consumed — a swallowed failure here strands the payment with funds held.
func (e *Engine) CompleteFromGateway(ctx context.Context, paymentID uuid.UUID, approved bool, pspRef, declineCode string) error {
	p, err := e.load(ctx, paymentID)
	if err != nil {
		return fmt.Errorf("load payment %s: %w", paymentID, err)
	}
	if p.Status != "processing" || p.Step != "submitted_to_gateway" {
		return nil // already resolved — a duplicate delivery, not a failure
	}
	if approved {
		if err := e.transition(ctx, p, "submitted_to_gateway", "processing", func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `update payments set psp_reference=$2 where id=$1`, p.ID, pspRef)
			return err
		}); err != nil {
			return fmt.Errorf("gateway completion persist: %w", err)
		}
		e.Advance(ctx, p.ID)
		return nil
	}
	e.failAndCompensate(ctx, p, "gateway_declined", declineCode)
	return nil
}

// ErrNotResumable means the payment is not parked awaiting step-up, so there is
// nothing for a resume to do.
var ErrNotResumable = errors.New("payment is not awaiting step-up")

// ResumeAfterStepUp re-drives a payment parked at requires_action once the caller
// has proven a second factor. Idempotent: a payment in any other state is
// rejected rather than re-driven, so a replayed resume cannot double-charge.
func (e *Engine) ResumeAfterStepUp(ctx context.Context, paymentID, userID uuid.UUID) error {
	p, err := e.load(ctx, paymentID)
	if err != nil {
		return err
	}
	// Never confirm another user's payment exists, and never resume a payment
	// that is not actually waiting on the challenge.
	if p.UserID != userID || p.Status != "requires_action" {
		return ErrNotResumable
	}
	// Fraud already screened it and asked for step-up; resume at the next step
	// rather than re-screening, which would only ask for it again.
	if err := e.transition(ctx, p, "fraud_screened", "processing", nil); err != nil {
		return err
	}
	go e.Advance(context.WithoutCancel(ctx), p.ID)
	return nil
}

// ExpireStaleRequiresAction fails payments whose step-up was never completed.
// No funds are held at this point (the hold comes after fraud screening), so
// expiry is a clean terminal transition with nothing to compensate.
func (e *Engine) ExpireStaleRequiresAction(ctx context.Context, timeout time.Duration) {
	tag, err := e.pool.Exec(ctx,
		`update payments
		    set status = 'failed', step = 'done',
		        failure_code = 'step_up_expired',
		        failure_detail = 'second factor was not completed in time',
		        updated_at = now()
		  where status = 'requires_action' and updated_at < now() - $1::interval`,
		timeout.String())
	if err != nil {
		e.log.Error("requires_action expiry failed", "error", err)
		return
	}
	if n := tag.RowsAffected(); n > 0 {
		e.log.Warn("expired payments awaiting step-up", "count", n, "timeout", timeout)
	}
}

// ExpireStuckSubmissions bounds the gateway-retry loop. A payment that cannot
// reach the processor stays retryable (see stepSubmit) instead of being falsely
// declined, but it must not retry forever: past the timeout it fails and the
// hold is released.
func (e *Engine) ExpireStuckSubmissions(ctx context.Context, timeout time.Duration) {
	rows, err := e.pool.Query(ctx,
		`select id from payments
		  where status = 'processing' and step = 'funds_held'
		    and updated_at < now() - $1::interval
		  limit 20`, timeout.String())
	if err != nil {
		e.log.Error("stuck-submission scan failed", "error", err)
		return
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	for _, id := range ids {
		p, err := e.load(ctx, id)
		if err != nil {
			continue
		}
		e.log.Warn("gateway unreachable past timeout — compensating", "payment_id", id, "timeout", timeout)
		e.failAndCompensate(ctx, p, "gateway_unavailable", "processor unreachable past the submission timeout")
	}
}

// ExpireStuckCaptures bounds the wait for a processor that will not confirm the
// capture. It scans ONLY payments whose PSP capture has not been confirmed
// (step is still submitted_to_gateway), where the hold is intact and no ledger
// movement has happened — so releasing the hold is a clean, complete correction.
//
// It deliberately cannot see step='psp_captured': releasing a hold after the
// processor has taken the money would refund the customer money we no longer
// hold, which is the very divergence this reordering exists to prevent.
func (e *Engine) ExpireStuckCaptures(ctx context.Context, timeout time.Duration) {
	rows, err := e.pool.Query(ctx,
		`select id from payments
		  where status = 'processing' and step = 'submitted_to_gateway'
		    and psp_reference is not null
		    and updated_at < now() - $1::interval
		  limit 20`, timeout.String())
	if err != nil {
		e.log.Error("stuck-capture scan failed", "error", err)
		return
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	for _, id := range ids {
		p, err := e.load(ctx, id)
		if err != nil {
			continue
		}
		e.log.Warn("processor never confirmed the capture — releasing hold", "payment_id", id, "timeout", timeout)
		e.failAndCompensate(ctx, p, "gateway_capture_failed", "processor did not confirm the capture in time")
	}
}

// AlertStalledLedgerCaptures surfaces the one residual of capturing at the
// processor first: it has the money and our ledger has not recorded it yet.
// This state is never auto-compensated — it is retried until it lands — so the
// only correct action here is to make it loud rather than let it sit silently.
func (e *Engine) AlertStalledLedgerCaptures(ctx context.Context, olderThan time.Duration) {
	var count int
	if err := e.pool.QueryRow(ctx,
		`select count(*) from payments
		  where status = 'processing' and step = 'psp_captured'
		    and updated_at < now() - $1::interval`, olderThan.String()).Scan(&count); err != nil {
		e.log.Error("stalled ledger-capture scan failed", "error", err)
		return
	}
	if count > 0 {
		e.log.Error("PSP captured but the ledger has not recorded it — money is collected and unbooked",
			"count", count, "older_than", olderThan)
	}
}

// ExpireStaleWallets fails wallet payments whose processor never reported an
// outcome. Without this an abandoned wallet authorization sits at
// submitted_to_gateway forever with the funds held, until the account-service
// hold sweeper expires it days later and a late completion can no longer capture.
func (e *Engine) ExpireStaleWallets(ctx context.Context, timeout time.Duration) {
	rows, err := e.pool.Query(ctx,
		`select id from payments
		  where status = 'processing' and step = 'submitted_to_gateway'
		    and method = 'wallet' and psp_reference is null
		    and updated_at < now() - $1::interval
		  limit 20`, timeout.String())
	if err != nil {
		e.log.Error("wallet expiry scan failed", "error", err)
		return
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	for _, id := range ids {
		p, err := e.load(ctx, id)
		if err != nil {
			continue
		}
		e.log.Warn("wallet completion timed out — compensating", "payment_id", id, "timeout", timeout)
		e.failAndCompensate(ctx, p, "gateway_timeout", "wallet processor did not report an outcome")
	}
}

// WriteRequestedEvent records payments.payment.requested.v1 in the caller's
// transaction (the same one inserting the payment row).
func (e *Engine) WriteRequestedEvent(ctx context.Context, tx pgx.Tx, paymentID, userID uuid.UUID,
	merchantID string, amount int64, currency string, fxQuoteID *uuid.UUID) error {
	payload := map[string]any{
		"payment_id":         paymentID.String(),
		"user_id":            userID.String(),
		"merchant_id":        merchantID,
		"amount_minor_units": amount,
		"currency_code":      currency,
	}
	if fxQuoteID != nil {
		payload["fx_quote_id"] = fxQuoteID.String()
	}
	return e.outbox.Write(ctx, tx, "payments.payment.requested.v1", "payment", paymentID.String(), payload)
}

// ── individual steps ─────────────────────────────────────────────────────────

func (e *Engine) stepFraud(ctx context.Context, p *Payment) bool {
	verdict := e.fraud.Screen(ctx, &fraudv1.ScoreRequest{
		PaymentId:       p.ID.String(),
		UserId:          p.UserID.String(),
		AccountId:       p.AccountID.String(),
		AmountMinorUnits: p.Amount,
		CurrencyCode:    p.Currency,
		MerchantId:      p.MerchantID,
		PaymentMethod:   p.Method,
		RequestedAtUnixMs: time.Now().UnixMilli(),
	})
	switch {
	case verdict.RequiresAction:
		if err := e.transitionWith(ctx, p, "fraud_screened", "requires_action", "", verdict.Reason, intPtr(int(verdict.RiskScore)), nil); err != nil {
			e.log.Error("requires_action transition failed", "payment_id", p.ID, "error", err)
		}
		return false // saga pauses; step-up flow re-initiates
	case !verdict.Proceed:
		e.failAndCompensate(ctx, p, "fraud_denied", verdict.Reason)
		return false
	default:
		err := e.transition(ctx, p, "fraud_screened", "processing", func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `update payments set risk_score=$2 where id=$1`, p.ID, verdict.RiskScore)
			return err
		})
		return err == nil
	}
}

func (e *Engine) stepHold(ctx context.Context, p *Payment) bool {
	holdID, err := e.accounts.Hold(ctx, p.ID.String(), p.AccountID.String(), p.Amount, p.Currency)
	if err != nil {
		if accountclient.IsInsufficientFunds(err) {
			e.failAndCompensate(ctx, p, "insufficient_funds", "available balance too low")
			return false
		}
		e.log.Error("hold failed", "payment_id", p.ID, "error", err)
		return false // stays processing; resume sweeper retries (idempotent request id)
	}
	hid := uuid.MustParse(holdID)
	err = e.transition(ctx, p, "funds_held", "processing", func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `update payments set hold_id=$2 where id=$1`, p.ID, hid)
		return err
	})
	return err == nil
}

func (e *Engine) stepSubmit(ctx context.Context, p *Payment) bool {
	processor, err := e.psps.For(p.Method)
	if err != nil {
		e.failAndCompensate(ctx, p, "gateway_declined", err.Error())
		return false
	}
	result, err := processor.Authorize(ctx, p.ID.String(), p.Amount, p.Currency, p.Token)
	if err != nil {
		if errors.Is(err, psp.ErrUnavailable) || errors.Is(err, context.DeadlineExceeded) {
			// Unreachable is NOT declined. Leave the payment retryable and let the
			// resume sweeper try again with the same idempotent gateway reference;
			// ExpireStuckSubmissions fails it if the processor stays down. Failing
			// here would turn a brief PSP blip into a permanent customer decline.
			e.log.Warn("gateway unavailable — will retry", "payment_id", p.ID, "error", err)
			e.touch(ctx, p.ID)
			return false
		}
		e.failAndCompensate(ctx, p, "gateway_declined", err.Error())
		return false
	}
	switch result.Outcome {
	case psp.OutcomeDeclined:
		e.failAndCompensate(ctx, p, "gateway_declined", result.DeclineCode)
		return false
	case psp.OutcomePending:
		if err := e.transition(ctx, p, "submitted_to_gateway", "processing", nil); err != nil {
			e.log.Error("pending transition failed", "payment_id", p.ID, "error", err)
		}
		return false // wait for gateway.psp.completed.v1
	default: // approved
		err := e.transition(ctx, p, "submitted_to_gateway", "processing", func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `update payments set psp_reference=$2 where id=$1`, p.ID, result.PSPReference); err != nil {
				return err
			}
			return e.outbox.Write(ctx, tx, "payments.payment.authorized.v1", "payment", p.ID.String(), map[string]any{
				"payment_id":         p.ID.String(),
				"hold_id":            deref(p.HoldID),
				"psp_reference":      result.PSPReference,
				"amount_minor_units": p.Amount,
				"currency_code":      p.Currency,
			})
		})
		return err == nil
	}
}

// stepPSPCapture takes the money at the processor BEFORE the ledger records it.
//
// The ledger is our record of what happened to the money, so posting a capture
// the processor has not confirmed asserts a fact that has not occurred. Doing it
// the other way round (the previous behaviour) meant a failed PSP capture left
// the merchant credited and the customer told "succeeded" for money the card
// network never collected — a silent, permanent divergence.
//
// Capturing first also makes compensation trivial: if the processor refuses, no
// ledger movement has happened, so releasing the still-active hold is the whole
// of the correction.
func (e *Engine) stepPSPCapture(ctx context.Context, p *Payment) bool {
	processor, err := e.psps.For(p.Method)
	if err != nil {
		e.failAndCompensate(ctx, p, "gateway_declined", err.Error())
		return false
	}
	if err := processor.Capture(ctx, *p.PSPReference); err != nil {
		if errors.Is(err, psp.ErrUnavailable) || errors.Is(err, context.DeadlineExceeded) {
			// Unreachable is not refused. Retry with the same PSP reference (the
			// capture is idempotent); ExpireStuckCaptures bounds the wait. The hold
			// is still intact, so nothing is stranded meanwhile.
			e.log.Warn("PSP capture unavailable — will retry", "payment_id", p.ID, "error", err)
			e.touch(ctx, p.ID)
			return false
		}
		e.failAndCompensate(ctx, p, "gateway_capture_failed", err.Error())
		return false
	}
	// Durably record that the processor HAS the money, before we try to write the
	// ledger. A resumed worker must never have to guess this: past this point the
	// hold must never be released, or we would refund money the network took.
	if err := e.transition(ctx, p, "psp_captured", "processing", nil); err != nil {
		e.log.Error("psp_captured checkpoint failed", "payment_id", p.ID, "error", err)
		return false
	}
	return true
}

// stepLedgerCapture posts the capture the processor already confirmed. It is the
// local, transactional half, keyed by a deterministic request id, so it is safe
// to retry until it lands — and it is never compensated away, because the money
// really is gone from the card network's side.
func (e *Engine) stepLedgerCapture(ctx context.Context, p *Payment) bool {
	ledgerTxn, err := e.accounts.Capture(ctx, p.ID.String(), p.HoldID.String(), p.Amount, p.Currency)
	if err != nil {
		// Retried by the resume sweeper with the same request id. This is the one
		// residual of capturing externally first: the processor has the money and
		// our books do not say so yet. It is visible, non-terminal and converges;
		// AlertStalledLedgerCaptures makes it loud if it does not.
		e.log.Error("ledger capture failed after PSP captured — retrying", "payment_id", p.ID, "error", err)
		e.touch(ctx, p.ID)
		return false
	}
	fxRate := e.fxRateUsed(ctx, p)
	err = e.terminalTx(ctx, p, "captured", "succeeded", func(tx pgx.Tx) error {
		return e.outbox.Write(ctx, tx, "payments.payment.captured.v1", "payment", p.ID.String(), map[string]any{
			"payment_id":            p.ID.String(),
			"user_id":               p.UserID.String(),
			"account_id":            p.AccountID.String(),
			"merchant_id":           p.MerchantID,
			"hold_id":               deref(p.HoldID),
			"ledger_transaction_id": ledgerTxn,
			"psp_reference":         derefS(p.PSPReference),
			"amount_minor_units":    p.Amount,
			"currency_code":         p.Currency,
			"fx_rate_used":          fxRate,
		})
	})
	return err == nil
}

// ── persistence helpers ──────────────────────────────────────────────────────

// touch bumps updated_at so a payment that just failed an attempt is not
// re-picked by ResumeStale on the very next tick — the retry backs off by the
// sweeper's staleness window instead of hot-looping against a dead processor.
func (e *Engine) touch(ctx context.Context, id uuid.UUID) {
	if _, err := e.pool.Exec(ctx, `update payments set updated_at = now() where id = $1`, id); err != nil {
		e.log.Error("touch failed", "payment_id", id, "error", err)
	}
}

func (e *Engine) load(ctx context.Context, id uuid.UUID) (*Payment, error) {
	p := &Payment{}
	err := e.pool.QueryRow(ctx, `
		select p.id, p.user_id, p.account_id, p.merchant_id, p.instrument_id,
		       i.method, i.gateway_token, p.amount, p.currency, p.fx_quote_id,
		       p.status, p.step, p.hold_id, p.psp_reference, p.risk_score
		  from payments p join instruments i on i.id = p.instrument_id
		 where p.id = $1`, id).
		Scan(&p.ID, &p.UserID, &p.AccountID, &p.MerchantID, &p.InstrumentID,
			&p.Method, &p.Token, &p.Amount, &p.Currency, &p.FxQuoteID,
			&p.Status, &p.Step, &p.HoldID, &p.PSPReference, &p.RiskScore)
	return p, err
}

// transition moves step (status stays processing) + runs extra writes in tx.
func (e *Engine) transition(ctx context.Context, p *Payment, step, status string, extra func(pgx.Tx) error) error {
	err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`update payments set step=$2, status=$3, version=version+1, updated_at=now()
			  where id=$1 and step=$4`, p.ID, step, status, p.Step)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("concurrent saga transition on %s", p.ID)
		}
		if extra != nil {
			return extra(tx)
		}
		return nil
	})
	if err == nil {
		e.bus.Publish(pubsub.Update{PaymentID: p.ID.String(), Status: status, Step: step})
	}
	return err
}

func (e *Engine) terminalTx(ctx context.Context, p *Payment, step, status string, extra func(pgx.Tx) error) error {
	return e.transitionWith(ctx, p, step, status, "", "", nil, extra)
}

func intPtr(v int) *int { return &v }

func (e *Engine) transitionWith(ctx context.Context, p *Payment, step, status, failureCode, failureDetail string, riskScore *int, extra func(pgx.Tx) error) error {
	err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			update payments set step=$2, status=$3,
			       failure_code=nullif($4,''), failure_detail=nullif($5,''),
			       risk_score=coalesce($6, risk_score),
			       version=version+1, updated_at=now()
			 where id=$1 and status='processing'`,
			p.ID, step, status, failureCode, failureDetail, riskScore)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("payment %s no longer processing", p.ID)
		}
		if extra != nil {
			return extra(tx)
		}
		return nil
	})
	if err == nil {
		e.bus.Publish(pubsub.Update{PaymentID: p.ID.String(), Status: status, Step: step})
	}
	return err
}

// failAndCompensate releases any hold, marks the payment failed and emits
// payments.payment.failed.v1 — the compensation path of the saga.
func (e *Engine) failAndCompensate(ctx context.Context, p *Payment, failureCode, detail string) {
	if p.HoldID != nil {
		if err := e.accounts.Release(ctx, p.ID.String(), p.HoldID.String(), "compensation"); err != nil {
			// account-service also consumes payment.failed and releases
			// orphaned holds — the event below is the safety net.
			e.log.Error("compensating release failed (event-driven fallback will retry)",
				"payment_id", p.ID, "error", err)
		}
	}
	err := e.transitionWith(ctx, p, "done", "failed", failureCode, detail, nil, func(tx pgx.Tx) error {
		return e.outbox.Write(ctx, tx, "payments.payment.failed.v1", "payment", p.ID.String(), map[string]any{
			"payment_id":         p.ID.String(),
			"user_id":            p.UserID.String(),
			"failure_code":       failureCode,
			"failure_detail":     detail,
			"amount_minor_units": p.Amount,
			"currency_code":      p.Currency,
		})
	})
	if err != nil {
		e.log.Error("failure transition failed", "payment_id", p.ID, "error", err)
	}
}

func (e *Engine) fxRateUsed(ctx context.Context, p *Payment) any {
	if p.FxQuoteID == nil {
		return nil
	}
	var rate string
	if err := e.pool.QueryRow(ctx,
		`select rate::text from fx_quotes where id=$1`, *p.FxQuoteID).Scan(&rate); err != nil {
		return nil
	}
	return rate
}

func deref(id *uuid.UUID) string {
	if id == nil {
		return ""
	}
	return id.String()
}

func derefS(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
