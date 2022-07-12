package agents

import (
	"context"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/smart_contract_agent/internal/state"
)

func (a *Agent) GetChannel(ctx context.Context,
	channelHash bitcoin.Hash32) (*state.Channel, error) {

	return nil, nil
}
