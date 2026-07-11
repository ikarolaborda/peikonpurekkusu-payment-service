package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// authStrength is what the gateway proved about the caller. Traefik overwrites
// X-Auth-* from the ForwardAuth response, so these cannot be forged by a client.
type authStrength struct {
	amr      []string
	authTime time.Time
}

func readAuthStrength(r *http.Request) authStrength {
	var s authStrength
	for _, m := range strings.Split(r.Header.Get("X-Auth-Amr"), ",") {
		if m = strings.TrimSpace(m); m != "" {
			s.amr = append(s.amr, m)
		}
	}
	// An absent or unparsable X-Auth-Time leaves the zero time, which is always
	// stale — missing evidence of a recent login must never read as a fresh one.
	if secs, err := strconv.ParseInt(r.Header.Get("X-Auth-Time"), 10, 64); err == nil {
		s.authTime = time.Unix(secs, 0)
	}
	return s
}

func (s authStrength) steppedUp() bool {
	for _, m := range s.amr {
		if m == "mfa" {
			return true
		}
	}
	return false
}

func (s authStrength) fresh(maxAge time.Duration) bool {
	return !s.authTime.IsZero() && time.Since(s.authTime) <= maxAge
}

// stepUpRequired reports whether this payment demands a second factor. Only
// high-risk payments do: at or above the amount limit, the caller must have
// completed MFA *recently* — an old-but-valid token must not move large money.
// Ordinary payments are unaffected, so a normal session is not punished for age.
//
// Fails closed: absent or unparsable X-Auth-* headers read as "not stepped up".
func (s *Server) stepUpRequired(r *http.Request, amountMinorUnits int64) bool {
	if amountMinorUnits < s.stepUpAmountLimit {
		return false
	}
	auth := readAuthStrength(r)
	return !(auth.steppedUp() && auth.fresh(s.stepUpMaxAge))
}
