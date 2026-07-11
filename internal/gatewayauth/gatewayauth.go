// Package gatewayauth verifies the signed gateway assertion that user-service
// mints at ForwardAuth, so this service trusts identity from a cryptographic
// claim rather than from raw X-User-Id — which any peer on the internal network
// could otherwise forge (Traefik only overwrites it at the edge).
//
// The assertion is an ES256 JWT (aud "peikon-internal") signed by the platform's
// sole private key holder, user-service; we verify it against its published
// JWKS. On success the verified claims are written back onto the request as the
// X-User-* headers the handlers already read, so handler code is unchanged but
// the values now provably originate from ForwardAuth.
package gatewayauth

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// audience pins the assertion type: an access token (a different audience)
// signed by the same key ring can never be replayed here as an identity proof.
const audience = "peikon-internal"

// assertionHeader carries the signed proof; identityHeaders are the derived
// values handlers read. All are stripped from the inbound request before the
// verified values are set, so a forged inbound copy cannot survive.
const assertionHeader = "X-Gateway-Assertion"

var identityHeaders = []string{"X-User-Id", "X-User-Roles", "X-Session-Id", "X-Auth-Amr", "X-Auth-Time"}

// Verifier holds the JWKS keyfunc (self-refreshing, refetches on unknown kid).
type Verifier struct {
	keyfunc jwt.Keyfunc
	log     *slog.Logger
}

// New builds a verifier against the JWKS URL. It retries the initial fetch
// because a downstream service may boot before user-service is reachable;
// once built, keyfunc refreshes in the background for the process lifetime.
func New(ctx context.Context, jwksURL string, log *slog.Logger) (*Verifier, error) {
	var lastErr error
	for attempt := 1; attempt <= 10; attempt++ {
		k, err := keyfunc.NewDefaultCtx(ctx, []string{jwksURL})
		if err == nil {
			return &Verifier{keyfunc: k.Keyfunc, log: log}, nil
		}
		lastErr = err
		log.Warn("gateway JWKS not ready, retrying", "url", jwksURL, "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, fmt.Errorf("gateway JWKS unreachable after retries: %w", lastErr)
}

// Middleware verifies the assertion on every non-health request and fails closed:
// a missing, malformed, expired, wrong-audience, wrong-algorithm, or unverifiable
// assertion is 401, never a pass-through.
func (v *Verifier) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/health/") {
			next.ServeHTTP(w, r)
			return
		}

		raw := r.Header.Get(assertionHeader)
		if raw == "" {
			v.reject(w, "missing gateway assertion")
			return
		}

		token, err := jwt.Parse(raw, v.keyfunc,
			jwt.WithValidMethods([]string{"ES256"}),
			jwt.WithAudience(audience),
			jwt.WithExpirationRequired(),
			jwt.WithLeeway(10*time.Second),
		)
		if err != nil || !token.Valid {
			v.reject(w, "invalid gateway assertion")
			v.log.Warn("gateway assertion rejected", "error", err)
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			v.reject(w, "invalid gateway assertion")
			return
		}
		sub, err := claims.GetSubject()
		if err != nil || sub == "" {
			v.reject(w, "invalid gateway assertion")
			return
		}

		// Verified identity is the only identity: drop any inbound copy first,
		// then set from claims so handlers read trusted values.
		for _, h := range identityHeaders {
			r.Header.Del(h)
		}
		r.Header.Del(assertionHeader)
		r.Header.Set("X-User-Id", sub)
		r.Header.Set("X-User-Roles", joinStrings(claims["roles"]))
		r.Header.Set("X-Auth-Amr", joinStrings(claims["amr"]))
		if at, ok := claims["auth_time"].(float64); ok {
			r.Header.Set("X-Auth-Time", strconv.FormatInt(int64(at), 10))
		}
		if sid, ok := claims["sid"].(string); ok {
			r.Header.Set("X-Session-Id", sid)
		}

		next.ServeHTTP(w, r)
	})
}

func (v *Verifier) reject(w http.ResponseWriter, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte(`{"title":"Unauthorized","status":401,"detail":"` + detail + `"}`))
}

// joinStrings renders a JSON string array claim as a comma-joined header value,
// matching the format user-service's ForwardAuth response used.
func joinStrings(v any) string {
	arr, ok := v.([]any)
	if !ok {
		return ""
	}
	parts := make([]string, 0, len(arr))
	for _, e := range arr {
		if s, ok := e.(string); ok {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, ",")
}
