package saga

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Reconciler compares what the processor says it collected against what our
// ledger says it booked. This is the safety net the capture reordering (B3)
// could not provide on its own: the saga guarantees we never book a capture the
// processor refused, but it cannot see money the processor took and we failed to
// record. Only the processor's own settlement report can.
//
// Detection only. It never moves money — an automated "correction" to a
// discrepancy in a payment ledger is exactly the thing that should need a human.
type Reconciler struct {
	engine  *Engine
	pspBase string
	http    *http.Client

	// Settlement is compared only against captures older than this. A payment
	// mid-flight is legitimately settled-but-not-yet-booked for a moment; without
	// a grace window the reconciler would cry drift on every healthy payment.
	grace time.Duration
}

func NewReconciler(engine *Engine, pspBaseURL string, grace time.Duration) *Reconciler {
	return &Reconciler{
		engine:  engine,
		pspBase: pspBaseURL,
		http:    &http.Client{Timeout: 10 * time.Second},
		grace:   grace,
	}
}

type settlementReport struct {
	// The period the processor's report covers. A capture we booked BEFORE this
	// is simply not in scope: absent-from-the-report means "not collected" only
	// for payments the report actually claims to cover. Without this the job
	// reports every pre-report payment as a violated invariant — a false alarm
	// that would train an operator to ignore the one alert that matters.
	ReportSince string            `json:"report_since"`
	Captures    []settlementEntry `json:"captures"`
}

type settlementEntry struct {
	Reference   string `json:"reference"`
	AmountMinor int64  `json:"amount_minor_units"`
	CapturedAt  string `json:"captured_at"`
	Settled     int    `json:"settled"`
}

// Run reports both directions of drift, because they mean opposite things.
func (r *Reconciler) Run(ctx context.Context) {
	report, err := r.fetchSettlement(ctx)
	if err != nil {
		r.engine.log.Error("reconciliation: settlement report unavailable", "error", err)
		return
	}
	settled := report.Captures

	coverageFrom, err := time.Parse(time.RFC3339, report.ReportSince)
	if err != nil {
		r.engine.log.Error("reconciliation: settlement report has no usable coverage period", "error", err)
		return
	}

	booked, outOfScope, err := r.bookedReferences(ctx, coverageFrom)
	if err != nil {
		r.engine.log.Error("reconciliation: could not read booked captures", "error", err)
		return
	}

	cutoff := time.Now().Add(-r.grace)
	var settledNotBooked, doubleSettled int
	settledRefs := make(map[string]struct{}, len(settled))

	for _, s := range settled {
		settledRefs[s.Reference] = struct{}{}

		// A processor that settled the same authorization twice would mean a real
		// double charge. The saga retries capture on resume, so this is the assertion
		// that those retries stay free.
		if s.Settled > 1 {
			doubleSettled++
			r.engine.log.Error("reconciliation: processor settled one authorization more than once",
				"psp_reference", s.Reference, "settled", s.Settled)
		}

		at, err := time.Parse(time.RFC3339, s.CapturedAt)
		if err != nil || at.After(cutoff) {
			continue // still inside the grace window — not yet evidence of anything
		}
		if _, ok := booked[s.Reference]; !ok {
			// The processor has the customer's money and our books do not say so.
			// This is the crash-window residual actually materialising.
			settledNotBooked++
			r.engine.log.Error("reconciliation: MONEY COLLECTED BUT NOT BOOKED",
				"psp_reference", s.Reference, "amount_minor_units", s.AmountMinor, "captured_at", s.CapturedAt)
		}
	}

	var bookedNotSettled int
	for ref := range booked {
		if _, ok := settledRefs[ref]; !ok {
			// We credited a merchant for money the processor never collected. The
			// capture reordering is supposed to make this impossible, so it is not a
			// warning — it is an alarm on a broken invariant.
			bookedNotSettled++
			r.engine.log.Error("reconciliation: BOOKED BUT NEVER SETTLED — capture-ordering invariant violated",
				"psp_reference", ref)
		}
	}

	if settledNotBooked == 0 && bookedNotSettled == 0 && doubleSettled == 0 {
		r.engine.log.Info("reconciliation clean",
			"settled", len(settled), "booked_in_scope", len(booked), "outside_report_coverage", outOfScope)
		return
	}
	r.engine.log.Error("reconciliation found drift",
		"settled_not_booked", settledNotBooked,
		"booked_not_settled", bookedNotSettled,
		"double_settled", doubleSettled)
}

func (r *Reconciler) fetchSettlement(ctx context.Context) (settlementReport, error) {
	var body settlementReport
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.pspBase+"/settlement", nil)
	if err != nil {
		return body, err
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return body, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return body, fmt.Errorf("settlement report: HTTP %d", resp.StatusCode)
	}
	err = json.NewDecoder(resp.Body).Decode(&body)
	return body, err
}

// bookedReferences are the captures our ledger recorded WITHIN the period the
// settlement report covers, plus a count of those that fall outside it — reported
// rather than dropped, so the blind spot is visible instead of merely quiet.
func (r *Reconciler) bookedReferences(ctx context.Context, coverageFrom time.Time) (map[string]struct{}, int, error) {
	rows, err := r.engine.pool.Query(ctx,
		`select psp_reference, updated_at from payments
		  where step = 'captured' and psp_reference is not null`)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	booked := map[string]struct{}{}
	outOfScope := 0
	for rows.Next() {
		var ref string
		var bookedAt time.Time
		if err := rows.Scan(&ref, &bookedAt); err != nil {
			return nil, 0, err
		}
		if bookedAt.Before(coverageFrom) {
			outOfScope++
			continue
		}
		booked[ref] = struct{}{}
	}
	return booked, outOfScope, rows.Err()
}
