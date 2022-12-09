package agents

import (
	"context"

	"github.com/tokenized/logger"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/messages"

	"github.com/pkg/errors"
)

func (a *Agent) processMessage(ctx context.Context, transaction *state.Transaction,
	message *actions.Message, outputIndex int) error {

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

	senderIndex := 0
	if len(message.SenderIndexes) == 1 {
		senderIndex = int(message.SenderIndexes[0])
	}

	transaction.Lock()

	outputCount := transaction.OutputCount()
	if outputCount <= receiverIndex {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Int("receiver_index", receiverIndex),
			logger.Int("output_count", outputCount),
		}, "Invalid message receivers index")

		transaction.Unlock()

		// No reject action necessary because we can't even confirm if this tx was addressed to this
		// agent.
		return nil
	}

	output := transaction.Output(receiverIndex)
	receiverLockingScript := output.LockingScript

	inputCount := transaction.InputCount()
	if inputCount <= senderIndex {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Int("sender_index", senderIndex),
			logger.Int("input_count", inputCount),
		}, "Invalid message senders index")

		transaction.Unlock()

		// No reject action necessary because we can't even confirm who to respond to.
		return nil
	}

	inputOutput, err := transaction.InputOutput(senderIndex)
	if err != nil {
		return errors.Wrapf(err, "input output %d", senderIndex)
	}

	senderLockingScript := inputOutput.LockingScript
	senderUnlockingScipt := transaction.Input(senderIndex).UnlockingScript

	transaction.Unlock()

	agentLockingScript := a.LockingScript()
	if !receiverLockingScript.Equal(agentLockingScript) {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("receiver_locking_script", receiverLockingScript),
		}, "Agent is not the message receiver")

		// This might be an important message related to an ongoing multi-contract transfer.
		return a.processNonRelevantMessage(ctx, transaction, message)
	}

	if len(message.SenderIndexes) > 1 {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "too many sender indexes")),
			"reject")
	}

	payload, err := messages.Deserialize(message.MessageCode, message.MessagePayload)
	if err != nil {
		logger.Warn(ctx, "Failed to deserialize message payload : %s", err)
		return nil
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.String("message_type", payload.TypeName()),
	}, "Message type")

	switch p := payload.(type) {
	case *messages.SettlementRequest:
		if err := a.processSettlementRequest(ctx, transaction, outputIndex, p, senderLockingScript,
			senderUnlockingScipt); err != nil {
			return errors.Wrapf(err, "settlement request")
		}

	case *messages.SignatureRequest:
		if err := a.processSignatureRequest(ctx, transaction, outputIndex, p, senderLockingScript,
			senderUnlockingScipt); err != nil {
			return errors.Wrapf(err, "settlement request")
		}
	}

	return nil
}

func (a *Agent) processNonRelevantMessage(ctx context.Context, transaction *state.Transaction,
	message *actions.Message) error {

	transaction.Lock()
	firstInput := transaction.Input(0)
	transaction.Unlock()

	previousTransaction, err := a.caches.Transactions.Get(ctx, firstInput.PreviousOutPoint.Hash)
	if err != nil {
		return errors.Wrap(err, "get tx")
	}

	if previousTransaction == nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("previous_txid", firstInput.PreviousOutPoint.Hash),
		}, "Previous transaction not found")
		return nil
	}
	defer a.caches.Transactions.Release(ctx, firstInput.PreviousOutPoint.Hash)

	return nil
}
