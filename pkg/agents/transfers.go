package agents

import (
	"context"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/specification/dist/golang/actions"
)

func (a *Agent) processTransfer(ctx context.Context, transaction TransactionWithOutputs,
	index int, transfer *actions.Transfer) error {

	logger.Info(ctx, "Processing transfer")

	return nil
}

func (a *Agent) processSettlement(ctx context.Context, transaction TransactionWithOutputs,
	index int, settlement *actions.Settlement) error {

	logger.Info(ctx, "Processing settlement")

	return nil
}
