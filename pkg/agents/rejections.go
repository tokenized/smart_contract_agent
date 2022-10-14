package agents

import (
	"context"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

// sendRejection creates a reject message transaction that spends the specified output and contains
// the specified reject code.
func (a *Agent) sendRejection(ctx context.Context, transaction *state.Transaction,
	rejectError platform.RejectError) error {

	agentLockingScript := a.LockingScript()
	rejectTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	transaction.Lock()

	if rejectError.OutputIndex != -1 {
		// Add input spending output flagged for response.
		outpoint := wire.OutPoint{
			Hash:  transaction.TxID(),
			Index: uint32(rejectError.OutputIndex),
		}
		output := transaction.Output(rejectError.OutputIndex)

		if output.LockingScript.Equal(agentLockingScript) {
			if err := rejectTx.AddInput(outpoint, output.LockingScript, output.Value); err != nil {
				transaction.Unlock()
				return errors.Wrap(err, "add response input")
			}
		}
	}

	// Find any other outputs with the contract locking script.
	outputCount := transaction.OutputCount()
	for i := 0; i < outputCount; i++ {
		if i == rejectError.OutputIndex {
			continue
		}

		output := transaction.Output(i)
		if output.LockingScript.Equal(agentLockingScript) {
			outpoint := wire.OutPoint{
				Hash:  transaction.TxID(),
				Index: uint32(i),
			}

			if err := rejectTx.AddInput(outpoint, output.LockingScript, output.Value); err != nil &&
				errors.Cause(err) != txbuilder.ErrDuplicateInput {
				transaction.Unlock()
				return errors.Wrap(err, "add input")
			}
		}
	}

	if len(rejectTx.MsgTx.TxIn) == 0 {
		logger.Warn(ctx, "No contract outputs found for rejection")
		transaction.Unlock()
		return nil
	}

	// Add output with locking script that had the issue. This is referenced by the
	// RejectAddressIndex zero value of the rejection action.
	rejectInputOutput, err := transaction.InputOutput(rejectError.InputIndex)
	if err != nil {
		transaction.Unlock()
		return errors.Wrap(err, "reject input output")
	}

	if err := rejectTx.AddOutput(rejectInputOutput.LockingScript, 1, false, true); err != nil {
		transaction.Unlock()
		return errors.Wrap(err, "add action")
	}

	// Add rejection action
	rejection := &actions.Rejection{
		RejectionCode: rejectError.Code,
		Message:       rejectError.Message,
		Timestamp:     rejectError.Timestamp,
	}

	rejectionScript, err := protocol.Serialize(rejection, a.IsTest())
	if err != nil {
		transaction.Unlock()
		return errors.Wrap(err, "serialize rejection")
	}

	if err := rejectTx.AddOutput(rejectionScript, 0, false, false); err != nil {
		transaction.Unlock()
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
			transaction.Unlock()
			return errors.Wrapf(err, "input output %d", i)
		}

		if refundInputValue < inputOutput.Value {
			refundInputValue = inputOutput.Value
			refundScript = inputOutput.LockingScript
		}
	}
	transaction.Unlock()

	if refundInputValue == 0 {
		return errors.New("No refund input found")
	}

	if err := rejectTx.SetChangeLockingScript(refundScript, ""); err != nil {
		return errors.Wrap(err, "set change locking script")
	}

	if _, err := rejectTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding for reject : %s", err)
			return nil
		}

		return errors.Wrap(err, "sign")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("response_txid", rejectTx.MsgTx.TxHash()),
		logger.Uint32("reject_code", rejectError.Code),
		logger.String("reject_label", rejectError.Label()),
		logger.String("reject_message", rejectError.Message),
	}, "Responding with rejection")
	if err := a.BroadcastTx(ctx, rejectTx.MsgTx); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	return nil
}
