package agents

import (
	"context"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

// sendReject creates a reject message transaction that spends the specified output and contains the
// specified reject code.
func (a *Agent) sendReject(ctx context.Context, transaction *state.Transaction,
	responseInput, responseOutput int, rejectCode int, message string, now uint64) error {

	tx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	outpoint := wire.OutPoint{
		Hash:  transaction.TxID(),
		Index: uint32(responseOutput),
	}
	output := transaction.Output(responseOutput)

	if err := tx.AddInput(outpoint, output.LockingScript, output.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	// Add output with locking script that had the issue. This is referenced by the
	// RejectAddressIndex zero value of the rejection action.
	rejectInputOutput, err := transaction.InputOutput(responseInput)
	if err != nil {
		return errors.Wrap(err, "reject input output")
	}

	if err := tx.AddOutput(rejectInputOutput.LockingScript, 1, false, true); err != nil {
		return errors.Wrap(err, "add action")
	}

	// Add rejection action
	rejection := &actions.Rejection{
		RejectionCode: uint32(rejectCode),
		Message:       message,
		Timestamp:     now,
	}

	rejectionScript, err := protocol.Serialize(rejection, a.IsTest())
	if err != nil {
		return errors.Wrap(err, "serialize rejection")
	}

	if err := tx.AddOutput(rejectionScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add action")
	}

	// Set locking script of largest input to receive any bitcoin change. Assume it is the main
	// funding and is the best option to receive any extra bitcoin.
	inputCount := transaction.InputCount()
	var refundInputValue uint64
	var refundScript bitcoin.Script
	for i := 0; i < inputCount; i++ {
		inputOutput, err := transaction.InputOutput(i)
		if err != nil {
			return errors.Wrapf(err, "input output %d", i)
		}

		if refundInputValue < inputOutput.Value {
			refundInputValue = inputOutput.Value
			refundScript = inputOutput.LockingScript
		}
	}

	if refundInputValue == 0 {
		return errors.New("No refund input found")
	}

	if err := tx.SetChangeLockingScript(refundScript, ""); err != nil {
		return errors.Wrap(err, "set change locking script")
	}

	if _, err := tx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		return errors.Wrap(err, "sign")
	}

	if err := a.BroadcastTx(ctx, tx.MsgTx); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	return nil
}
