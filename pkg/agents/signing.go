package agents

import (
	"context"
	"fmt"

	"github.com/tokenized/bitcoin_interpreter"
	"github.com/tokenized/bitcoin_interpreter/p2pkh"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/txbuilder"
	"github.com/tokenized/txbuilder/fees"

	"github.com/pkg/errors"
)

func (a *Agent) Sign(ctx context.Context, tx *txbuilder.TxBuilder,
	changeLockingScript bitcoin.Script, additionalUnlockers ...bitcoin_interpreter.Unlocker) error {

	inputsValue, err := fees.InputsValue(tx)
	if err != nil {
		return errors.Wrap(err, "inputs value")
	}

	outputsValue := fees.OutputsValue(tx)

	fee := int64(inputsValue) - int64(outputsValue)

	unlockers := bitcoin_interpreter.MultiUnlocker{
		p2pkh.NewUnlocker(a.Key()), // Default signer
	}

	if len(additionalUnlockers) > 0 {
		// Unlockers for any special inputs like agent bitcoin transfers.
		unlockers = append(unlockers, additionalUnlockers...)
	}

	config := a.Config()
	feeEstimate, err := fees.EstimateFee(tx, unlockers, config.FeeRate)
	if err != nil {
		return errors.Wrap(err, "estimate fee")
	}

	if fee < int64(feeEstimate) {
		description := fmt.Sprintf("%d funding < %d needed + %d tx fee", inputsValue,
			outputsValue, feeEstimate)
		logger.Warn(ctx, "Insufficient tx funding : %s", description)
		return errors.Wrapf(txbuilder.ErrInsufficientValue, description)
	}

	if fee > int64(feeEstimate) { // There is some change
		change := uint64(fee) - feeEstimate
		found := false

		// Add change to the contract fee output if there is one.
		for _, txout := range tx.MsgTx.TxOut {
			if txout.LockingScript.Equal(changeLockingScript) {
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Uint64("change", change),
				}, "Adding change to existing output")

				found = true
				txout.Value += change
				break
			}
		}

		if !found {
			outputFee, dust := fees.OutputFeeAndDustForLockingScript(changeLockingScript,
				config.DustFeeRate, config.FeeRate)

			if change > outputFee+dust {
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Uint64("change", change),
				}, "Adding new output for change")

				if err := tx.AddOutput(changeLockingScript, change-outputFee, true,
					false); err != nil {
					return errors.Wrap(err, "add change output")
				}
			} else {
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Uint64("output_fee", outputFee),
					logger.Uint64("dust", dust),
					logger.Uint64("change", change),
				}, "Change below cost of new output and dust")
			}
		}
	}

	// Some transactions will have inputs from other agents so it is okay if we can't sign all the
	// inputs. We must always sign at least one input or something went wrong. We still return the
	// "can't unlock" error so that the caller knows the tx is not fully unlocked.
	var cantUnlock error
	unlockedCount := 0
	for inputIndex, txin := range tx.MsgTx.TxIn {
		unlockingScript, err := unlockers.Unlock(ctx, tx, inputIndex, 0)
		if err != nil {
			if errors.Cause(err) == bitcoin_interpreter.CantUnlock {
				cantUnlock = errors.Wrapf(err, "unlock input %d", inputIndex)
				continue
			}
			return errors.Wrapf(err, "unlock input %d", inputIndex)
		}
		txin.UnlockingScript = unlockingScript
		unlockedCount++
	}

	if unlockedCount == 0 {
		return errors.New("Failed to sign transaction")
	}

	return cantUnlock
}
