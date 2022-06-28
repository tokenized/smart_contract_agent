package agents

import (
	"context"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
)

func (a *Agent) processContractFormation(ctx context.Context, transaction *state.Transaction,
	index int, formation *actions.ContractFormation) error {

	if index != 0 {
		logger.Warn(ctx, "Contract formation not from input zero: %d", index)
		return nil
	}

	logger.Info(ctx, "Processing contract formation")

	a.contract.Lock()

	a.contract.Unlock()

	return nil
}
