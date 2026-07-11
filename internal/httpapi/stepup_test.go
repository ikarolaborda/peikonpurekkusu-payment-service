package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

const (
	limit  = int64(100_000)
	maxAge = 15 * time.Minute
)

func request(amr string, authTime time.Time) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/payments", nil)
	if amr != "" {
		r.Header.Set("X-Auth-Amr", amr)
	}
	if !authTime.IsZero() {
		r.Header.Set("X-Auth-Time", strconv.FormatInt(authTime.Unix(), 10))
	}
	return r
}

func server() *Server {
	return &Server{stepUpAmountLimit: limit, stepUpMaxAge: maxAge}
}

func TestStepUpNotRequiredBelowLimit(t *testing.T) {
	s := server()
	// An ordinary payment on a password-only session must still go through: the
	// gate is for high-risk money, not a tax on every purchase.
	if s.stepUpRequired(request("pwd", time.Now()), limit-1) {
		t.Fatal("small payment must not demand step-up")
	}
	// Even an old session may make a small payment.
	if s.stepUpRequired(request("pwd", time.Now().Add(-24*time.Hour)), 1250) {
		t.Fatal("small payment on an aged session must not demand step-up")
	}
}

func TestStepUpRequiredAtOrAboveLimitWithoutMfa(t *testing.T) {
	s := server()
	if !s.stepUpRequired(request("pwd", time.Now()), limit) {
		t.Fatal("large payment without mfa must demand step-up")
	}
}

func TestStepUpRequiredWhenMfaIsStale(t *testing.T) {
	s := server()
	// An old-but-valid session must not move large money on a stale second factor.
	if !s.stepUpRequired(request("pwd,mfa", time.Now().Add(-maxAge-time.Minute)), limit) {
		t.Fatal("large payment with a stale mfa must demand step-up")
	}
}

func TestStepUpSatisfiedByRecentMfa(t *testing.T) {
	s := server()
	if s.stepUpRequired(request("pwd,mfa", time.Now()), limit*10) {
		t.Fatal("recent mfa must satisfy step-up for a large payment")
	}
}

func TestStepUpFailsClosedOnMissingOrJunkHeaders(t *testing.T) {
	s := server()
	// Absent headers must never read as "recently stepped up".
	if !s.stepUpRequired(request("", time.Time{}), limit) {
		t.Fatal("missing auth headers must fail closed")
	}
	r := request("pwd,mfa", time.Time{})
	r.Header.Set("X-Auth-Time", "not-a-number")
	if !s.stepUpRequired(r, limit) {
		t.Fatal("unparsable X-Auth-Time must fail closed, not pass as fresh")
	}
}
