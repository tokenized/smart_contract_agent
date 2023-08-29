package agents

import (
	"context"
	"fmt"

	"github.com/tokenized/bitcoin_interpreter"
	"github.com/tokenized/bitcoin_interpreter/check_signature_preimage"
	"github.com/tokenized/bitcoin_interpreter/p2pkh"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
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
		var deficit uint64
		if fee > 0 {
			deficit = feeEstimate - uint64(fee)
		} else {
			deficit = feeEstimate + uint64(-fee)
		}

		logger.WarnWithFields(ctx, []logger.Field{
			logger.Uint64("inputs", inputsValue),
			logger.Uint64("outputs", outputsValue),
			logger.Uint64("needed_tx_fee", feeEstimate),
			logger.Int64("current_tx_fee", fee),
			logger.Uint64("deficit", deficit),
		}, "Insufficient tx funding")

		if isReject(tx, config.IsTest) {
			// Change fee locking script output.
			feeLockingScript := a.FeeLockingScript()
			for _, txout := range tx.MsgTx.TxOut {
				if !txout.LockingScript.Equal(feeLockingScript) {
					continue
				}

				dust := fees.DustLimitForLockingScript(feeLockingScript, config.DustFeeRate)
				if txout.Value > dust && txout.Value-dust > deficit {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Uint64("deficit", deficit),
						logger.Uint64("previous_value", txout.Value),
						logger.Uint64("new_value", txout.Value-deficit),
					}, "Reducing contract fee to fund rejection")
					txout.Value -= deficit
					fee += int64(deficit)
				}
			}

			if fee < int64(feeEstimate) {
				// Remove rejection message. This should probably only happen with zero contract fee
				// since otherwise the tx fee would have already been taken from the fee output.
				if removeRejectionMessage(ctx, tx, config.IsTest) {
					// Recalculate fee estimate based on smaller transaction size.
					sizeEstimate, err = fees.EstimateSize(tx, unlockers)
					if err != nil {
						return errors.Wrap(err, "estimate size")
					}

					feeEstimate = fees.EstimateFeeValue(sizeEstimate, config.FeeRate)
					if err != nil {
						return errors.Wrap(err, "estimate fee")
					}
				}
			}
		}

		if fee < int64(feeEstimate) {
			description := fmt.Sprintf("%d funding < %d (+%d tx fee) needed", inputsValue,
				outputsValue, feeEstimate)
			return errors.Wrapf(txbuilder.ErrInsufficientValue, description)
		}
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

func isReject(tx *txbuilder.TxBuilder, isTest bool) bool {
	for _, txout := range tx.MsgTx.TxOut {
		act, err := protocol.Deserialize(txout.LockingScript, isTest)
		if err != nil {
			continue
		}

		if _, ok := act.(*actions.Rejection); ok {
			return true
		}
	}

	return false
}

func removeRejectionMessage(ctx context.Context, tx *txbuilder.TxBuilder, isTest bool) bool {
	println("removing rejection message")
	for _, txout := range tx.MsgTx.TxOut {
		act, err := protocol.Deserialize(txout.LockingScript, isTest)
		if err != nil {
			continue
		}

		rejection, ok := act.(*actions.Rejection)
		if !ok {
			continue
		}

		logger.WarnWithFields(ctx, []logger.Field{
			logger.String("message", rejection.Message),
		}, "Removing rejection message to fix funding")
		rejection.Message = ""

		script, err := protocol.Serialize(rejection, isTest)
		if err != nil {
			return false
		}

		txout.LockingScript = script
		return true
	}

	return false
}
