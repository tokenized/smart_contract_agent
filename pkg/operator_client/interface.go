package operator_client

import (
	"context"
	"errors"

	"github.com/tokenized/channels/contract_operator"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
)

var (
	ErrInvalidResponse = errors.New("Invalid Response")
	ErrTimeout         = errors.New("Timeout")
)

// Client is the interface for interacting with an contract operator service.
type Client interface {
	// RequestNewAgent requests a new smart contract agent be created.
	// feeLockingScript and masterLockingScript can be left empty and will be provided by the
	// contract operator.
	RequestNewAgent(ctx context.Context,
		adminLockingScript, feeLockingScript, masterLockingScript bitcoin.Script,
		minimumContractFee uint64) (*contract_operator.Agent, error)

	// SignContractOffer adds a signed input and an output to a contract offer transaction.
	// The input will be added as the second input so it is the contract's "operator" input.
	// Then an output to retrieve the value in the input will be added so the agent isn't losing
	// bitcoin.
	// These have to be accounted for in the tx fee before calling this function because, since
	// the input will be signed, no other changes can be made to the tx other than signing inputs
	// without invalidating the operator input's signature.
	SignContractOffer(ctx context.Context,
		etx *expanded_tx.ExpandedTx) (*expanded_tx.ExpandedTx, error)
}
