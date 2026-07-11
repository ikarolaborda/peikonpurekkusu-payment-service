// Package accountclient wraps the account-service gRPC money API. Request ids
// are derived deterministically from the payment id, so saga retries and
// resumes can never double-hold, double-capture or double-release.
package accountclient

import (
	"context"
	"time"

	accountv1 "github.com/ikarolaborda/peikonpurekkusu-contracts/gen/go/account/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Client struct {
	client accountv1.AccountServiceClient
}

func New(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{client: accountv1.NewAccountServiceClient(conn)}, nil
}

// IsInsufficientFunds distinguishes the business decline from infra failure.
func IsInsufficientFunds(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}

func (c *Client) Hold(ctx context.Context, paymentID, accountID string, amount int64, currency string) (holdID string, err error) {
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := c.client.HoldFunds(callCtx, &accountv1.HoldFundsRequest{
		RequestId: "hold:" + paymentID,
		AccountId: accountID,
		PaymentId: paymentID,
		Amount:    &accountv1.Money{MinorUnits: amount, CurrencyCode: currency},
	})
	if err != nil {
		return "", err
	}
	return resp.GetHoldId(), nil
}

func (c *Client) Capture(ctx context.Context, paymentID, holdID string, amount int64, currency string) (ledgerTxnID string, err error) {
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := c.client.CaptureFunds(callCtx, &accountv1.CaptureFundsRequest{
		RequestId: "capture:" + paymentID,
		HoldId:    holdID,
		Amount:    &accountv1.Money{MinorUnits: amount, CurrencyCode: currency},
	})
	if err != nil {
		return "", err
	}
	return resp.GetLedgerTransactionId(), nil
}

func (c *Client) Release(ctx context.Context, paymentID, holdID, reason string) error {
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := c.client.ReleaseFunds(callCtx, &accountv1.ReleaseFundsRequest{
		RequestId: "release:" + paymentID,
		HoldId:    holdID,
		Reason:    reason,
	})
	// releasing an already-released/captured hold is a no-op for the saga
	if status.Code(err) == codes.FailedPrecondition {
		return nil
	}
	return err
}
