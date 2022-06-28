package agents

import (
	"context"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/specification/dist/golang/actions"
)

func (a *Agent) processFreeze(ctx context.Context, transaction TransactionWithOutputs,
	index int, freeze *actions.Freeze) error {

	if index != 0 {
		logger.Warn(ctx, "Freeze not from input zero: %d", index)
		return nil
	}

	logger.Info(ctx, "Processing freeze")

	return nil
}

func (a *Agent) processThaw(ctx context.Context, transaction TransactionWithOutputs,
	index int, thaw *actions.Thaw) error {

	if index != 0 {
		logger.Warn(ctx, "Thaw not from input zero: %d", index)
		return nil
	}

	logger.Info(ctx, "Processing thaw")

	return nil
}

func (a *Agent) processConfiscation(ctx context.Context, transaction TransactionWithOutputs,
	index int, confiscation *actions.Confiscation) error {

	if index != 0 {
		logger.Warn(ctx, "Confiscation not from input zero: %d", index)
		return nil
	}

	logger.Info(ctx, "Processing confiscation")

	return nil
}

func (a *Agent) processReconciliation(ctx context.Context, transaction TransactionWithOutputs,
	index int, reconciliation *actions.Reconciliation) error {

	if index != 0 {
		logger.Warn(ctx, "Reconciliation not from input zero: %d", index)
		return nil
	}

	logger.Info(ctx, "Processing reconciliation")

	return nil
}
