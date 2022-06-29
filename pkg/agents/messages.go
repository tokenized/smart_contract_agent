package agents

import (
	"context"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/specification/dist/golang/actions"
)

func (a *Agent) processMessage(ctx context.Context, transaction TransactionWithOutputs,
	message *actions.Message) error {

	// TODO Verify appropriate input or output belongs to this contract.
	// for _, senderIndex := range act.SenderIndexes {
	// 	if int(senderIndex) >= tx.InputCount() {
	// 		return fmt.Errorf("Message sender index out of range : %d >= %d", senderIndex,
	// 			tx.InputCount())
	// 	}

	// 	lockingScript, err := tx.InputLockingScript(int(senderIndex))
	// 	if err != nil {
	// 		return errors.Wrap(err, "input locking script")
	// 	}

	// 	if err := f.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
	// 		return errors.Wrap(err, "add agent action")
	// 	}
	// }

	// for _, receiverIndex := range act.ReceiverIndexes {
	// 	if int(receiverIndex) >= tx.OutputCount() {
	// 		return fmt.Errorf("Message receiver index out of range : %d >= %d", receiverIndex,
	// 			tx.OutputCount())
	// 	}

	// 	lockingScript := tx.Output(int(receiverIndex)).LockingScript
	// 	if err := f.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
	// 		return errors.Wrap(err, "add agent action")
	// 	}
	// }

	logger.Info(ctx, "Processing message")

	return nil
}

func (a *Agent) processRejection(ctx context.Context, transaction TransactionWithOutputs,
	rejection *actions.Rejection) error {

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
