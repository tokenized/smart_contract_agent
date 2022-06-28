package agents

import (
	"context"

	"github.com/pkg/errors"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/specification/dist/golang/actions"
)

func (a *Agent) processBodyOfAgreementFormation(ctx context.Context,
	transaction TransactionWithOutputs, index int,
	formation *actions.BodyOfAgreementFormation) error {

	if index != 0 {
		logger.Warn(ctx, "Body Of Agreement formation not from input zero: %d", index)
		return nil
	}

	logger.Info(ctx, "Processing body of agreement formation")

	a.contract.Lock()

	if a.contract.BodyOfAgreementFormation != nil &&
		formation.Timestamp < a.contract.BodyOfAgreementFormation.Timestamp {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(formation.Timestamp)),
			logger.Timestamp("existing_timestamp",
				int64(a.contract.BodyOfAgreementFormation.Timestamp)),
		}, "Older body of agreement formation")
	}

	a.contract.BodyOfAgreementFormation = formation
	txid := transaction.TxID()
	a.contract.BodyOfAgreementFormationTxID = &txid

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Timestamp("timestamp", int64(formation.Timestamp)),
	}, "Updated body of agreement formation")

	a.contract.Unlock()

	if err := a.contracts.Save(ctx, a.contract); err != nil {
		return errors.Wrap(err, "save contract")
	}

	return nil
}
