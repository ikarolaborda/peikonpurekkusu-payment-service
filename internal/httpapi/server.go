// Package httpapi serves the public payments API (behind Traefik ForwardAuth
// + gateway CSRF): idempotent initiation, status reads, SSE live updates,
// FX quotes and demo instruments. PaymentsFacade sits between transport and
// the saga engine (Facade pattern).
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/peikonpurekkusu/payment-service/internal/idempotency"
	"github.com/peikonpurekkusu/payment-service/internal/pubsub"
	"github.com/peikonpurekkusu/payment-service/internal/saga"
)

type Server struct {
	pool    *pgxpool.Pool
	idem    *idempotency.Store
	engine  *saga.Engine
	bus     *pubsub.Bus
	quoteTTL time.Duration
	kafkaOK func() bool
	log     *slog.Logger
}

func New(pool *pgxpool.Pool, idem *idempotency.Store, engine *saga.Engine, bus *pubsub.Bus,
	quoteTTL time.Duration, kafkaOK func() bool, log *slog.Logger) *Server {
	return &Server{pool: pool, idem: idem, engine: engine, bus: bus, quoteTTL: quoteTTL, kafkaOK: kafkaOK, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /health/ready", s.ready)
	mux.HandleFunc("POST /payments", s.createPayment)
	mux.HandleFunc("GET /payments/instruments", s.listInstruments)
	mux.HandleFunc("POST /payments/fx-quote", s.fxQuote)
	mux.HandleFunc("GET /payments/{id}", s.getPayment)
	mux.HandleFunc("GET /payments/{id}/events", s.streamEvents)
	mux.HandleFunc("GET /payments/merchants", s.listMerchants)
	return mux
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	status := http.StatusOK
	checks := map[string]string{"postgres": "up", "kafka": "up"}
	if err := s.pool.Ping(ctx); err != nil {
		checks["postgres"], status = "down", http.StatusServiceUnavailable
	}
	if !s.kafkaOK() {
		checks["kafka"], status = "down", http.StatusServiceUnavailable
	}
	writeJSON(w, status, checks)
}

type createPaymentReq struct {
	AccountID    string `json:"account_id"`
	MerchantID   string `json:"merchant_id"`
	InstrumentID string `json:"instrument_id"`
	FxQuoteID    string `json:"fx_quote_id"`
	Amount       struct {
		MinorUnits   int64  `json:"amount_minor_units"`
		CurrencyCode string `json:"currency_code"`
	} `json:"amount"`
}

func (s *Server) createPayment(w http.ResponseWriter, r *http.Request) {
	userID, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		httpError(w, http.StatusUnauthorized, "missing identity")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 255 {
		httpError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	if err != nil {
		httpError(w, http.StatusBadRequest, "unreadable body")
		return
	}

	replay, err := s.idem.Begin(r.Context(), userID, key, idempotency.HashBody(body))
	switch {
	case errors.Is(err, idempotency.ErrPayloadMismatch):
		httpError(w, http.StatusUnprocessableEntity, "idempotency key reused with different payload")
		return
	case errors.Is(err, idempotency.ErrInFlight):
		httpError(w, http.StatusConflict, "request in flight for this idempotency key")
		return
	case err != nil:
		s.fail(w, "idempotency begin", err)
		return
	case replay != nil:
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Idempotency-Replayed", "true")
		w.WriteHeader(replay.Code)
		_, _ = w.Write(replay.Body)
		return
	}

	code, resp := s.executeCreate(r.Context(), userID, body)
	respBody, _ := json.Marshal(resp)
	if err := s.idem.Complete(r.Context(), userID, key, code, respBody); err != nil {
		s.log.Error("idempotency complete failed", "error", err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(respBody)
}

// executeCreate validates, persists the payment aggregate (step=requested,
// with its payments.payment.requested.v1 outbox event) and launches the saga.
func (s *Server) executeCreate(ctx context.Context, userID uuid.UUID, body []byte) (int, any) {
	var req createPaymentReq
	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, problem("malformed JSON body")
	}
	accountID, err1 := uuid.Parse(req.AccountID)
	instrumentID, err2 := uuid.Parse(req.InstrumentID)
	if err1 != nil || err2 != nil || req.MerchantID == "" {
		return http.StatusBadRequest, problem("account_id, instrument_id and merchant_id are required")
	}
	if req.Amount.MinorUnits <= 0 || len(req.Amount.CurrencyCode) != 3 {
		return http.StatusBadRequest, problem("amount must be positive minor units with ISO-4217 currency")
	}
	var fxQuoteID *uuid.UUID
	if req.FxQuoteID != "" {
		id, err := uuid.Parse(req.FxQuoteID)
		if err != nil {
			return http.StatusBadRequest, problem("invalid fx_quote_id")
		}
		var expiresAt time.Time
		err = s.pool.QueryRow(ctx, `select expires_at from fx_quotes where id=$1`, id).Scan(&expiresAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return http.StatusBadRequest, problem("unknown fx_quote_id")
		}
		if time.Now().After(expiresAt) {
			return http.StatusBadRequest, problem("fx quote expired — request a new quote")
		}
		fxQuoteID = &id
	}

	paymentID := uuid.New()
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			insert into payments (id, user_id, account_id, merchant_id, instrument_id,
			                      amount, currency, fx_quote_id, status, step)
			values ($1,$2,$3,$4,$5,$6,$7,$8,'processing','requested')`,
			paymentID, userID, accountID, req.MerchantID, instrumentID,
			req.Amount.MinorUnits, req.Amount.CurrencyCode, fxQuoteID); err != nil {
			return err
		}
		return s.engine.WriteRequestedEvent(ctx, tx, paymentID, userID, req.MerchantID,
			req.Amount.MinorUnits, req.Amount.CurrencyCode, fxQuoteID)
	})
	if err != nil {
		s.log.Error("payment insert failed", "error", err)
		return http.StatusInternalServerError, problem("could not create payment")
	}

	go s.engine.Advance(context.WithoutCancel(ctx), paymentID)

	return http.StatusAccepted, map[string]any{
		"id":     paymentID.String(),
		"status": "processing",
		"amount": map[string]any{
			"amount_minor_units": req.Amount.MinorUnits,
			"currency_code":      req.Amount.CurrencyCode,
		},
		"merchant_id": req.MerchantID,
	}
}

func (s *Server) getPayment(w http.ResponseWriter, r *http.Request) {
	userID, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		httpError(w, http.StatusUnauthorized, "missing identity")
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid payment id")
		return
	}
	row := s.pool.QueryRow(r.Context(), `
		select p.id, p.status, p.step, p.amount, p.currency, p.merchant_id, m.display_name,
		       p.instrument_id, p.failure_code, p.failure_detail, p.psp_reference, p.created_at, p.updated_at
		  from payments p join merchants m on m.id = p.merchant_id
		 where p.id=$1 and p.user_id=$2`, id, userID)
	var (
		pid, status, step, currency, merchantID, merchantName string
		instrumentID                                          uuid.UUID
		amount                                                int64
		failureCode, failureDetail, pspRef                    *string
		createdAt, updatedAt                                  time.Time
	)
	if err := row.Scan(&pid, &status, &step, &amount, &currency, &merchantID, &merchantName,
		&instrumentID, &failureCode, &failureDetail, &pspRef, &createdAt, &updatedAt); err != nil {
		httpError(w, http.StatusNotFound, "payment not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": pid, "status": status, "step": step,
		"amount":        map[string]any{"amount_minor_units": amount, "currency_code": currency},
		"merchant_id":   merchantID,
		"merchant_name": merchantName,
		"instrument_id": instrumentID.String(),
		"failure_code":  failureCode,
		"failure_detail": failureDetail,
		"psp_reference": pspRef,
		"created_at":    createdAt, "updated_at": updatedAt,
	})
}

// streamEvents is the SSE feed of saga transitions for one payment.
func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request) {
	userID, err := uuid.Parse(r.Header.Get("X-User-Id"))
	if err != nil {
		httpError(w, http.StatusUnauthorized, "missing identity")
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid payment id")
		return
	}
	var status, step string
	if err := s.pool.QueryRow(r.Context(),
		`select status, step from payments where id=$1 and user_id=$2`, id, userID).
		Scan(&status, &step); err != nil {
		httpError(w, http.StatusNotFound, "payment not found")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		httpError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	send := func(u pubsub.Update) {
		payload, _ := json.Marshal(u)
		fmt.Fprintf(w, "event: status\ndata: %s\n\n", payload)
		flusher.Flush()
	}
	updates, unsubscribe := s.bus.Subscribe(id.String())
	defer unsubscribe()
	send(pubsub.Update{PaymentID: id.String(), Status: status, Step: step})

	heartbeat := time.NewTicker(25 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case u, open := <-updates:
			if !open {
				return
			}
			send(u)
			if u.Status != "processing" {
				return // terminal — close the stream
			}
		}
	}
}

func (s *Server) listInstruments(w http.ResponseWriter, r *http.Request) {
	rows, err := s.pool.Query(r.Context(),
		`select id, method, brand, last4, exp_month, exp_year from instruments order by method`)
	if err != nil {
		s.fail(w, "list instruments", err)
		return
	}
	defer rows.Close()
	type instrument struct {
		ID       string  `json:"instrument_id"`
		Method   string  `json:"method"`
		Brand    *string `json:"brand"`
		Last4    *string `json:"last4"`
		ExpMonth *int    `json:"exp_month"`
		ExpYear  *int    `json:"exp_year"`
	}
	out := []instrument{}
	for rows.Next() {
		var i instrument
		if err := rows.Scan(&i.ID, &i.Method, &i.Brand, &i.Last4, &i.ExpMonth, &i.ExpYear); err != nil {
			s.fail(w, "scan instrument", err)
			return
		}
		out = append(out, i)
	}
	writeJSON(w, http.StatusOK, map[string]any{"instruments": out})
}

func (s *Server) listMerchants(w http.ResponseWriter, r *http.Request) {
	rows, err := s.pool.Query(r.Context(), `select id, display_name, category from merchants order by display_name`)
	if err != nil {
		s.fail(w, "list merchants", err)
		return
	}
	defer rows.Close()
	type merchant struct {
		ID          string `json:"merchant_id"`
		DisplayName string `json:"display_name"`
		Category    string `json:"category"`
	}
	out := []merchant{}
	for rows.Next() {
		var m merchant
		if err := rows.Scan(&m.ID, &m.DisplayName, &m.Category); err != nil {
			s.fail(w, "scan merchant", err)
			return
		}
		out = append(out, m)
	}
	writeJSON(w, http.StatusOK, map[string]any{"merchants": out})
}

type fxQuoteReq struct {
	Amount struct {
		MinorUnits   int64  `json:"amount_minor_units"`
		CurrencyCode string `json:"currency_code"`
	} `json:"amount"`
	TargetCurrency string `json:"target_currency"`
}

// fxQuote locks the latest rate for a pair until expires_at. Inverse pairs
// are derived (rates are stored single-direction, append-only).
func (s *Server) fxQuote(w http.ResponseWriter, r *http.Request) {
	if _, err := uuid.Parse(r.Header.Get("X-User-Id")); err != nil {
		httpError(w, http.StatusUnauthorized, "missing identity")
		return
	}
	var req fxQuoteReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "malformed body")
		return
	}
	base, target := req.Amount.CurrencyCode, req.TargetCurrency
	if len(base) != 3 || len(target) != 3 || base == target {
		httpError(w, http.StatusBadRequest, "distinct ISO-4217 base and target currencies required")
		return
	}

	var rateID uuid.UUID
	var rate string
	var inverted bool
	err := s.pool.QueryRow(r.Context(), `
		select id, rate::text from exchange_rates
		 where base=$1 and quote=$2 order by valid_from desc limit 1`, base, target).
		Scan(&rateID, &rate)
	if errors.Is(err, pgx.ErrNoRows) {
		err = s.pool.QueryRow(r.Context(), `
			select id, (1/rate)::numeric(18,8)::text from exchange_rates
			 where base=$1 and quote=$2 order by valid_from desc limit 1`, target, base).
			Scan(&rateID, &rate)
		inverted = true
	}
	if err != nil {
		httpError(w, http.StatusBadRequest, "no rate available for pair")
		return
	}
	_ = inverted

	quoteID := uuid.New()
	expiresAt := time.Now().Add(s.quoteTTL)
	if _, err := s.pool.Exec(r.Context(), `
		insert into fx_quotes (id, base, quote, rate, rate_id, expires_at)
		values ($1,$2,$3,$4::numeric,$5,$6)`,
		quoteID, base, target, rate, rateID, expiresAt); err != nil {
		s.fail(w, "quote insert", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"quote_id":   quoteID.String(),
		"base":       base,
		"quote":      target,
		"rate":       rate,
		"expires_at": expiresAt.UTC(),
	})
}

func (s *Server) fail(w http.ResponseWriter, op string, err error) {
	s.log.Error(op, "error", err)
	httpError(w, http.StatusInternalServerError, "internal error")
}

func problem(detail string) map[string]any {
	return map[string]any{"title": "Bad Request", "status": 400, "detail": detail}
}

func httpError(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]any{"title": http.StatusText(status), "status": status, "detail": detail})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
