package agents

import (
	"context"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/specification/dist/golang/actions"
)

func (a *Agent) processIncomingMessage(ctx context.Context, transaction TransactionWithOutputs,
	index int, message *actions.Message) error {

	logger.Info(ctx, "Processing incoming message")

	return nil
}

func (a *Agent) processIncomingRejection(ctx context.Context, transaction TransactionWithOutputs,
	index int, rejection *actions.Rejection) error {

	logger.Info(ctx, "Processing incoming rejection")

	return nil
}

func (a *Agent) processOutgoingRejection(ctx context.Context, transaction TransactionWithOutputs,
	index int, rejection *actions.Rejection) error {

	logger.Info(ctx, "Processing outgoing rejection")

	return nil
}
