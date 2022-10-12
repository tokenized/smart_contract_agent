package agents

import (
	"context"

	"github.com/pkg/errors"
	"github.com/tokenized/logger"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/messages"
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
	if len(message.ReceiverIndexes) == 1 {
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
		}, "Contract is not the message receiver")
		return nil
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

func (a *Agent) processRejection(ctx context.Context, transaction *state.Transaction,
	rejection *actions.Rejection, now uint64) error {

	// TODO Verify appropriate input or output belongs to this contract.
	// inputCount := tx.InputCount()
	// for i := 0; i < inputCount; i++ {
	// 	lockingScript, err := tx.InputLockingScript(i)
	// 	if err != nil {
	// 		return errors.Wrap(err, "input locking script")
	// 	}

	// 	if err := f.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
	// 		return errors.Wrap(err, "add agent action")
	// 	}
	// }

	// for _, addressIndex := range act.AddressIndexes {
	// 	if int(addressIndex) >= tx.OutputCount() {
	// 		return fmt.Errorf("Reject address index out of range : %d >= %d", addressIndex,
	// 			tx.OutputCount())
	// 	}

	// 	lockingScript := tx.Output(int(addressIndex)).LockingScript
	// 	if err := f.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
	// 		return errors.Wrap(err, "add agent action")
	// 	}
	// }

	logger.Info(ctx, "Processing outgoing rejection")

	return nil
}
