// Package fraudclient wraps the inline pre-authorization fraud check with the
// amount-tiered timeout policy (Strategy): a fraud-service outage fails OPEN
// for small amounts (payment proceeds, flagged for async review) and CLOSED
// at/above the configured limit. The policy lives here, with the caller —
// the fraud service itself only ever scores.
package fraudclient

import (
	"context"
	"log/slog"
	"time"

	fraudv1 "github.com/ikarolaborda/peikonpurekkusu-contracts/gen/go/fraud/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Verdict is the saga-facing decision.
type Verdict struct {
	Proceed       bool
	RequiresAction bool   // step-up / manual review
	RiskScore     int32
	FailedOpen    bool   // outage tolerated — flag for review
	Reason        string
}

type Client struct {
	client        fraudv1.FraudServiceClient
	deadline      time.Duration
	failOpenLimit int64
	log           *slog.Logger
}

func New(addr string, deadline time.Duration, failOpenLimit int64, log *slog.Logger) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{
		client:        fraudv1.NewFraudServiceClient(conn),
		deadline:      deadline,
		failOpenLimit: failOpenLimit,
		log:           log,
	}, nil
}

func (c *Client) Screen(ctx context.Context, req *fraudv1.ScoreRequest) Verdict {
	callCtx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()
	resp, err := c.client.Score(callCtx, req)
	if err != nil {
		// Timeout or unavailable → amount-tiered policy.
		if req.GetAmountMinorUnits() < c.failOpenLimit {
			c.log.Warn("fraud check unavailable — failing OPEN (flagged)",
				"payment_id", req.GetPaymentId(), "amount", req.GetAmountMinorUnits(), "error", err)
			return Verdict{Proceed: true, FailedOpen: true, RiskScore: -1, Reason: "fraud_unavailable_fail_open"}
		}
		c.log.Warn("fraud check unavailable — failing CLOSED",
			"payment_id", req.GetPaymentId(), "amount", req.GetAmountMinorUnits(), "error", err)
		return Verdict{Proceed: false, Reason: "fraud_unavailable_fail_closed"}
	}

	switch resp.GetDecision() {
	case fraudv1.Decision_DECISION_APPROVE:
		return Verdict{Proceed: true, RiskScore: resp.GetRiskScore()}
	case fraudv1.Decision_DECISION_STEP_UP, fraudv1.Decision_DECISION_HOLD:
		return Verdict{RequiresAction: true, RiskScore: resp.GetRiskScore(), Reason: "fraud_" + resp.GetDecision().String()}
	default: // DENY or unspecified
		return Verdict{Proceed: false, RiskScore: resp.GetRiskScore(), Reason: "fraud_denied"}
	}
}
