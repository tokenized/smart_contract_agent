package agents

import (
	"context"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
)

func (a *Agent) processBodyOfAgreementFormation(ctx context.Context, transaction *state.Transaction,
	index int, formation *actions.BodyOfAgreementFormation) error {

	if index != 0 {
		logger.Warn(ctx, "Body Of Agreement formation not from input zero: %d", index)
		return nil
	}

	logger.Info(ctx, "Processing body of agreement formation")

	return nil
}
