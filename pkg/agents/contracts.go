package agents

import (
	"context"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/specification/dist/golang/actions"

	"github.com/pkg/errors"
)

func (a *Agent) processContractFormation(ctx context.Context, transaction TransactionWithOutputs,
	index int, formation *actions.ContractFormation) error {

	if index != 0 {
		logger.Warn(ctx, "Contract formation not from input zero: %d", index)
		return nil
	}

	logger.Info(ctx, "Processing contract formation")

	a.contract.Lock()

	if a.contract.Formation != nil && formation.Timestamp < a.contract.Formation.Timestamp {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(formation.Timestamp)),
			logger.Timestamp("existing_timestamp", int64(a.contract.Formation.Timestamp)),
		}, "Older contract formation")
	}

	a.contract.Formation = formation
	txid := transaction.TxID()
	a.contract.FormationTxID = &txid

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Timestamp("timestamp", int64(formation.Timestamp)),
	}, "Updated contract formation")

	a.contract.Unlock()

	if err := a.contracts.Save(ctx, a.contract); err != nil {
		return errors.Wrap(err, "save contract")
	}

	return nil
}
