package agents

import (
	"context"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
)

func (a *Agent) processInstrumentCreation(ctx context.Context, transaction *state.Transaction,
	index int, creation *actions.InstrumentCreation) error {

	if index != 0 {
		logger.Warn(ctx, "Instrument creation not from input zero: %d", index)
		return nil
	}

	logger.Info(ctx, "Processing instrument creation")

	return nil
}
