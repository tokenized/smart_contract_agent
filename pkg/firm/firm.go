package firm

import (
	"context"
	"errors"

	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/smart_contract_agent/pkg/agent"
)

// Firm is a factory and repository for smart contract agents.
type Firm struct {
	wallet *wallet.Wallet
}

func (f *Firm) Run(ctx context.Context, interrupt chan<- interface{}) error {

	// Ensure all threads that use state.Cache finish before stopping Cache or they might get
	// ErrShuttingDown when requesting data.
	return errors.New("Not Implemented")
}

func (f *Firm) GetAgent(ctx context.Context, deriviationHash bitcoin.Hash32) (*agent.Agent, error) {
	return nil, errors.New("Not Implemented")
}
