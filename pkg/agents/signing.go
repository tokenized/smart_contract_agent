package agents

import (
	"context"
	"fmt"

	"github.com/tokenized/bitcoin_interpreter"
	"github.com/tokenized/bitcoin_interpreter/check_signature_preimage"
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

	// Unlockers for any special inputs like agent bitcoin transfers.
	for _, additionalUnlocker := range additionalUnlockers {
		if additionalUnlocker == nil {
			continue
		}
		unlockers = append(unlockers, additionalUnlocker)
	}

	sizeEstimate, err := fees.EstimateSize(tx, unlockers)
	if err != nil {
		return errors.Wrap(err, "estimate size")
	}

	config := a.Config()
	feeEstimate := fees.EstimateFeeValue(sizeEstimate, config.FeeRate)
	if err != nil {
		return errors.Wrap(err, "estimate fee")
	}

	if fee < int64(feeEstimate) {
		description := fmt.Sprintf("%d funding < %d needed + %d tx fee", inputsValue,
			outputsValue, feeEstimate)
		logger.Warn(ctx, "Insufficient tx funding : %s", description)
		return errors.Wrapf(txbuilder.ErrInsufficientValue, description)
	}

	if fee > int64(feeEstimate) && len(changeLockingScript) > 0 { // There is some change
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

	// Loop in case the tx needs malleation.
	for i := 0; i < 10; i++ {
		// Some transactions will have inputs from other agents so it is okay if we can't sign all
		// the inputs. We must always sign at least one input or something went wrong. We still
		// return the "can't unlock" error so that the caller knows the tx is not fully unlocked.
		unlockedCount := 0
		needsMalleation := false
		for inputIndex, txin := range tx.MsgTx.TxIn {
			unlockingScript, err := unlockers.Unlock(ctx, tx, inputIndex)
			if err != nil {
				if errors.Cause(err) == bitcoin_interpreter.CantUnlock ||
					errors.Cause(err) == bitcoin_interpreter.CantSign ||
					errors.Cause(err) == bitcoin_interpreter.ScriptNotMatching {
					continue
				}

				if errors.Cause(err) == check_signature_preimage.TxNeedsMalleation {
					needsMalleation = true
					break
				}

				return errors.Wrapf(err, "unlock input %d", inputIndex)
			}
			txin.UnlockingScript = unlockingScript
			unlockedCount++
		}

		if needsMalleation {
			// Clear all previous signatures.
			for _, txin := range tx.MsgTx.TxIn {
				txin.UnlockingScript = nil
			}

			tx.MsgTx.LockTime++
			continue
		}

		if unlockedCount == 0 {
			return errors.New("Failed to sign transaction")
		}

		break
	}

	return nil
}
