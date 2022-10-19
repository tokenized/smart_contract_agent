package agents

import (
	"context"

	"github.com/tokenized/logger"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/messages"

	"github.com/pkg/errors"
)

func (a *Agent) processMessage(ctx context.Context, transaction *state.Transaction,
	message *actions.Message, now uint64) error {

	// Verify appropriate output belongs to this contract.
	if len(message.ReceiverIndexes) > 1 {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Int("receiver_count", len(message.ReceiverIndexes)),
		}, "Unsupported number of message receivers")
		return nil
	}

	receiverIndex := 0
	if len(message.ReceiverIndexes) != 0 {
		receiverIndex = int(message.ReceiverIndexes[0])
	}

	transaction.Lock()

	outputCount := transaction.OutputCount()
	if outputCount <= receiverIndex {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Int("receiver_index", receiverIndex),
			logger.Int("output_count", outputCount),
		}, "Invalid message receivers index")

		transaction.Unlock()
		return nil
	}

	output := transaction.Output(receiverIndex)

	transaction.Unlock()

	agentLockingScript := a.LockingScript()
	if !output.LockingScript.Equal(agentLockingScript) {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("receiver_locking_script", output.LockingScript),
		}, "Agent is not the message receiver")

		// This might be an important message related to an ongoing multi-contract transfer.
		return a.processNonRelevantMessage(ctx, transaction, message, now)
	}

	logger.Info(ctx, "Processing message")

	payload, err := messages.Deserialize(message.MessageCode, message.MessagePayload)
	if err != nil {
		logger.Warn(ctx, "Failed to deserialize message payload : %s", err)
		return nil
	}

	switch p := payload.(type) {
	case *messages.SettlementRequest:
		if err := a.processSettlementRequest(ctx, transaction, p, now); err != nil {
			return errors.Wrapf(err, "settlement request")
		}

	case *messages.SignatureRequest:
		if err := a.processSignatureRequest(ctx, transaction, p, now); err != nil {
			return errors.Wrapf(err, "settlement request")
		}
	}

	return nil
}

func (a *Agent) processNonRelevantMessage(ctx context.Context, transaction *state.Transaction,
	message *actions.Message, now uint64) error {

	transaction.Lock()
	firstInput := transaction.Input(0)
	transaction.Unlock()

	previousTransaction, err := a.transactions.Get(ctx, firstInput.PreviousOutPoint.Hash)
	if err != nil {
		return errors.Wrap(err, "get tx")
	}

	if previousTransaction == nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("previous_txid", firstInput.PreviousOutPoint.Hash),
		}, "Previous transaction not found")
		return nil
	}
	defer a.transactions.Release(ctx, firstInput.PreviousOutPoint.Hash)

	return nil
}
