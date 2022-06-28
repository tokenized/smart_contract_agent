package agents

import (
	"context"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
)

func (a *Agent) processSettlement(ctx context.Context, transaction *state.Transaction,
	index int, settlement *actions.Settlement) error {

	logger.Info(ctx, "Processing settlement")

	return nil
}
