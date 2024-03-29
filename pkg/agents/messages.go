package agents

import (
	"context"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/messages"

	"github.com/pkg/errors"
)

func (a *Agent) processMessage(ctx context.Context, transaction *transactions.Transaction,
	message *actions.Message, actionIndex int) (*expanded_tx.ExpandedTx, error) {

	// Verify appropriate output belongs to this contract.
	if len(message.ReceiverIndexes) > 1 {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Int("receiver_count", len(message.ReceiverIndexes)),
		}, "Unsupported number of message receivers")
		return nil, nil
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

	txid := transaction.TxID()

	outputCount := transaction.OutputCount()
	if outputCount <= receiverIndex {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Int("receiver_index", receiverIndex),
			logger.Int("output_count", outputCount),
		}, "Invalid message receivers index")

		transaction.Unlock()

		// No reject action necessary because we can't even confirm if this tx was addressed to this
		// agent.
		return nil, nil
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
		return nil, nil
	}

	input := transaction.Input(senderIndex)
	requestTxID := input.PreviousOutPoint.Hash

	inputOutput, err := transaction.InputOutput(senderIndex)
	if err != nil {
		return nil, errors.Wrapf(err, "input output %d", senderIndex)
	}

	senderLockingScript := inputOutput.LockingScript
	senderUnlockingScipt := transaction.Input(senderIndex).UnlockingScript

	agentLockingScript := a.LockingScript()
	if agentLockingScript.Equal(senderLockingScript) {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("receiver_locking_script", output.LockingScript),
		}, "Agent is the message sender")
		if _, err := a.addResponseTxID(ctx, requestTxID, txid, message, actionIndex); err != nil {
			return nil, errors.Wrap(err, "add response txid")
		}

		transaction.Lock()
		transaction.SetProcessed(a.ContractHash(), actionIndex)
		transaction.Unlock()

		return nil, nil
	}

	transaction.Unlock()

	if !receiverLockingScript.Equal(agentLockingScript) {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("receiver_locking_script", receiverLockingScript),
		}, "Agent is not the message receiver")

		// This might be an important message related to an ongoing multi-contract transfer.
		if err := a.processNonRelevantMessage(ctx, transaction, message); err != nil {
			return nil, errors.Wrap(err, "non-relevant message")
		}

		return nil, nil
	}

	if len(message.SenderIndexes) > 1 {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"too many sender indexes")
	}

	payload, err := messages.Deserialize(message.MessageCode, message.MessagePayload)
	if err != nil {
		logger.Warn(ctx, "Failed to deserialize message payload : %s", err)
		return nil, nil
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint32("message_code", message.MessageCode),
		logger.String("message_type", payload.TypeName()),
	}, "Message type")

	var responseEtx *expanded_tx.ExpandedTx
	var processErr error
	switch p := payload.(type) {
	case *messages.SettlementRequest:
		responseEtx, processErr = a.processSettlementRequest(ctx, transaction, actionIndex, p,
			senderLockingScript, senderUnlockingScipt)
		if processErr != nil {
			processErr = errors.Wrap(processErr, "settlement request")
		}

	case *messages.SignatureRequest:
		responseEtx, processErr = a.processSignatureRequest(ctx, transaction, actionIndex, p,
			senderLockingScript, senderUnlockingScipt)
		if processErr != nil {
			processErr = errors.Wrap(processErr, "signature request")
		}

	default:
		logger.Warn(ctx, "Unsupported message type")
	}

	return responseEtx, processErr
}

func (a *Agent) processNonRelevantMessage(ctx context.Context, transaction *transactions.Transaction,
	message *actions.Message) error {

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

// Determine how exactly to send this rejection. If it is the master contract of a multi-contract
// transfer then we respond to the initial request. Otherwise we respond to the current request,
// which is just contract to contract.
func (a *Agent) createMessageRejection(ctx context.Context, transaction *transactions.Transaction,
	message *actions.Message, actionIndex int,
	rejectError platform.RejectError) ([]*expanded_tx.ExpandedTx, error) {

	payload, err := messages.Deserialize(message.MessageCode, message.MessagePayload)
	if err != nil {
		logger.Warn(ctx, "Failed to deserialize message payload : %s", err)
		return nil, nil
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint32("message_code", message.MessageCode),
		logger.String("message_type", payload.TypeName()),
	}, "Message type")

	var etxs []*expanded_tx.ExpandedTx
	switch p := payload.(type) {
	case *messages.SettlementRequest:
		etxs, err = a.createSettlementRequestRejection(ctx, transaction, actionIndex, p, rejectError)
		if err != nil {
			err = errors.Wrap(err, "settlement request")
		}

	case *messages.SignatureRequest:
		etxs, err = a.createSignatureRequestRejection(ctx, transaction, actionIndex, p, rejectError)
		if err != nil {
			err = errors.Wrap(err, "signature request")
		}

	default:
		logger.Warn(ctx, "Unsupported message type")
		err = nil
	}

	return etxs, err
}
