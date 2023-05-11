package agents

import (
	"context"

	"github.com/tokenized/bitcoin_interpreter"
	"github.com/tokenized/bitcoin_interpreter/agent_bitcoin_transfer"
	"github.com/tokenized/bitcoin_interpreter/p2pkh"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/messages"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"

	"github.com/pkg/errors"
)

func (a *Agent) processRejection(ctx context.Context, transaction *transactions.Transaction,
	rejection *actions.Rejection, outputIndex int) (*expanded_tx.ExpandedTx, error) {

	transaction.Lock()
	txid := transaction.TxID()
	firstInput := transaction.Input(0)
	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return nil, errors.Wrapf(err, "input locking script %d", 0)
	}
	rejectedTxID := firstInput.PreviousOutPoint.Hash

	receiverIndex := int(rejection.RejectAddressIndex)
	outputCount := transaction.OutputCount()
	if outputCount <= receiverIndex {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Int("receiver_index", receiverIndex),
			logger.Int("output_count", outputCount),
		}, "Invalid rejection receiver index")

		transaction.Unlock()
		return nil, nil
	}

	output := transaction.Output(receiverIndex)

	transaction.Unlock()

	agentLockingScript := a.LockingScript()
	if agentLockingScript.Equal(inputOutput.LockingScript) {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("receiver_locking_script", output.LockingScript),
		}, "Agent is the rejection sender")
		if _, err := a.addResponseTxID(ctx, rejectedTxID, txid, rejection,
			outputIndex); err != nil {
			return nil, errors.Wrap(err, "add response txid")
		}

		transaction.Lock()
		transaction.SetProcessed(a.ContractHash(), outputIndex)
		transaction.Unlock()

		return nil, nil
	}

	if !output.LockingScript.Equal(agentLockingScript) {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("receiver_locking_script", output.LockingScript),
		}, "Agent is not the rejection receiver")
		return nil, nil
	}

	// Check if this is a reject from another contract in a multi-contract transfer to a settlement
	// request.
	rejectedTransaction, err := a.transactions.Get(ctx, rejectedTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get tx")
	}

	if rejectedTransaction == nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("rejected_txid", rejectedTxID),
		}, "Rejected transaction not found")
		return nil, nil
	}
	defer a.transactions.Release(ctx, rejectedTxID)

	// Find action
	config := a.Config()
	rejectedTransaction.Lock()
	outputCount = rejectedTransaction.OutputCount()
	var message *actions.Message
	for i := 0; i < outputCount; i++ {
		output := rejectedTransaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, config.IsTest)
		if err != nil {
			continue
		}

		if m, ok := action.(*actions.Message); ok &&
			(m.MessageCode == messages.CodeSettlementRequest ||
				m.MessageCode == messages.CodeSignatureRequest) {
			message = m
			break
		}
	}
	rejectedTransaction.Unlock()

	if message == nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("rejected_txid", rejectedTxID),
		}, "Rejected transaction does not contain an action")
		return nil, nil
	}

	// Verify this is from the next contract in the transfer.
	transferTransaction, transfer, transferOutputIndex, err := a.traceToTransfer(ctx,
		firstInputTxID(rejectedTransaction))
	if err != nil {
		return nil, errors.Wrap(err, "trace to transfer")
	}

	if transferTransaction == nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("rejected_txid", rejectedTxID),
		}, "Rejected transaction does not trace to a transfer")
		return nil, nil
	}
	transferTxID := transferTransaction.GetTxID()
	defer a.transactions.Release(ctx, transferTxID)

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("transfer_txid", transferTxID),
	}, "Found related transfer transaction")

	transferContracts, err := parseTransferContracts(transferTransaction, transfer,
		agentLockingScript)
	if err != nil {
		if errors.Cause(err) == ErrNotRelevant {
			logger.Warn(ctx, "Transfer not relevant to this contract agent")
			return nil, nil // Not for this contract
		}

		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			if transferContracts != nil && transferContracts.FirstContractOutputIndex != -1 {
				rejectError.OutputIndex = transferContracts.FirstContractOutputIndex
			}

			if transferContracts != nil && transferContracts.IsFirstContract() {
				etx, rerr := a.createRejection(ctx, transferTransaction, transferOutputIndex,
					-1, rejectError)
				if rerr != nil {
					return nil, errors.Wrap(rerr, "reject")
				}

				return etx, nil
			}

			return nil, nil // Only first contract can reject at this point
		}

		return nil, errors.Wrap(err, "parse contracts")
	}

	if transferContracts.IsFirstContract() {
		// This is the first contract so create a reject for the transfer itself.
		etx, rerr := a.createRejection(ctx, transferTransaction, transferOutputIndex, -1,
			platform.NewRejectErrorWithOutputIndex(int(rejection.RejectionCode), rejection.Message,
				transferContracts.FirstContractOutputIndex))
		if rerr != nil {
			return nil, errors.Wrap(rerr, "reject")
		}

		return etx, nil
	}

	// This is not the first contract so create a reject to the previous contract agent.
	if len(transferContracts.PreviousLockingScript) == 0 {
		return nil, errors.New("Previous locking script missing for send signature request")
	}

	return nil, platform.NewRejectErrorFull(int(rejection.RejectionCode), rejection.Message, 0, -1,
		transferContracts.PreviousLockingScript)
}

func firstInputTxID(transaction *transactions.Transaction) bitcoin.Hash32 {
	transaction.Lock()
	firstInput := transaction.Input(0)
	txid := firstInput.PreviousOutPoint.Hash
	transaction.Unlock()

	return txid
}

func (a *Agent) traceToTransfer(ctx context.Context,
	txid bitcoin.Hash32) (*transactions.Transaction, *actions.Transfer, int, error) {

	previousTransaction, err := a.transactions.Get(ctx, txid)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "get tx")
	}

	config := a.Config()
	if previousTransaction != nil {
		// Find action
		previousTransaction.Lock()
		firstInput := previousTransaction.Input(0)
		previousTxID := firstInput.PreviousOutPoint.Hash
		outputCount := previousTransaction.OutputCount()
		var message *actions.Message
		for i := 0; i < outputCount; i++ {
			output := previousTransaction.Output(i)
			action, err := protocol.Deserialize(output.LockingScript, config.IsTest)
			if err != nil {
				continue
			}

			switch a := action.(type) {
			case *actions.Transfer:
				previousTransaction.Unlock()
				return previousTransaction, a, i, nil
			case *actions.Message:
				message = a
			}
		}
		previousTransaction.Unlock()
		a.transactions.Release(ctx, txid)

		if message != nil && (message.MessageCode == messages.CodeSettlementRequest ||
			message.MessageCode == messages.CodeSignatureRequest) {
			return a.traceToTransfer(ctx, previousTxID)
		}

		// This tx doesn't link back to a multi-contract transfer.
		return nil, nil, 0, nil
	}

	fetchedTx, err := a.FetchTx(ctx, txid)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "fetch tx")
	}

	if fetchedTx == nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("previous_txid", txid),
		}, "Previous transaction not found")
		return nil, nil, 0, nil
	}

	previousTxID := fetchedTx.TxIn[0].PreviousOutPoint.Hash

	// Find action
	var message *actions.Message
	for _, txout := range fetchedTx.TxOut {
		act, err := protocol.Deserialize(txout.LockingScript, config.IsTest)
		if err != nil {
			continue
		}

		switch a := act.(type) {
		case *actions.Transfer:
			// This transfer is not known by this agent because it isn't in the transaction storage.
			return nil, nil, 0, nil

		case *actions.Message:
			message = a
		}
	}

	if message != nil && (message.MessageCode == messages.CodeSettlementRequest ||
		message.MessageCode == messages.CodeSignatureRequest) {
		// This tx doesn't link back to a multi-contract transfer.
		return nil, nil, 0, nil
	}

	return a.traceToTransfer(ctx, previousTxID)
}

// createRejection creates a reject message transaction that spends the specified output and
// contains the specified reject code.
func (a *Agent) createRejection(ctx context.Context, transaction *transactions.Transaction,
	actionOutputIndex, responseOutputIndex int,
	rejectError error) (*expanded_tx.ExpandedTx, error) {

	var reject *platform.RejectError
	if re, ok := rejectError.(platform.RejectError); ok {
		reject = &re
	} else if re, ok := rejectError.(*platform.RejectError); ok {
		reject = re
	}

	agentLockingScript := a.LockingScript()
	changeLockingScript := a.FeeLockingScript()
	config := a.Config()
	rejectTx := txbuilder.NewTxBuilder(float32(config.FeeRate), float32(config.DustFeeRate))

	transaction.Lock()

	txid := transaction.TxID()

	actionOutput := transaction.Output(actionOutputIndex)
	action, err := protocol.Deserialize(actionOutput.LockingScript, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "deserialize action")
	}

	var agentBitcoinTransferUnlocker bitcoin_interpreter.Unlocker
	if transfer, ok := action.(*actions.Transfer); ok {
		var bitcoinTransfer *actions.InstrumentTransferField
		for _, inst := range transfer.Instruments {
			if inst.InstrumentType == protocol.BSVInstrumentID {
				bitcoinTransfer = inst
				break
			}
		}

		if bitcoinTransfer != nil {
			// Create an unlocker for the agent bitcoin transfer script.
			signer := p2pkh.NewUnlockerFull(a.Key(), true, bitcoin_interpreter.SigHashDefault, -1)
			agentBitcoinTransferUnlocker = agent_bitcoin_transfer.NewAgentRefundUnlocker(signer)

			// Find any agent bitcoin transfer outputs with the contract locking script.
			outputCount := transaction.OutputCount()
			for outputIndex := 0; outputIndex < outputCount; outputIndex++ {
				output := transaction.Output(outputIndex)
				if !agentBitcoinTransferUnlocker.CanUnlock(output.LockingScript) {
					continue
				}

				// Find appropriate output to add.
				info, err := agent_bitcoin_transfer.MatchScript(output.LockingScript)
				if err != nil {
					continue
				}

				// Find refund script
				var refundLockingScript bitcoin.Script

				recoverLockingScript := info.RecoverLockingScript.Copy()
				recoverLockingScript.RemoveHardVerify()
				println("recover locking script", recoverLockingScript.String())
				if info.RefundMatches(recoverLockingScript, output.Value) {
					refundLockingScript = recoverLockingScript
				}

				if len(refundLockingScript) == 0 {
					inputCount := transaction.InputCount()
					for subInputIndex := 0; subInputIndex < inputCount; subInputIndex++ {
						inputOutput, err := transaction.InputOutput(subInputIndex)
						if err != nil {
							continue
						}

						println("input script", inputOutput.LockingScript.String())
						if info.RefundMatches(inputOutput.LockingScript, output.Value) {
							refundLockingScript = inputOutput.LockingScript
							break
						}
					}
				}

				if len(refundLockingScript) == 0 {
					for subOutputIndex := 0; subOutputIndex < outputCount; subOutputIndex++ {
						subTxout := transaction.Output(subOutputIndex)
						println("output script", subTxout.LockingScript.String())
						if info.RefundMatches(subTxout.LockingScript, output.Value) {
							refundLockingScript = subTxout.LockingScript
							break
						}
					}
				}

				if len(refundLockingScript) == 0 {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Int("output_index", outputIndex),
					}, "Refund locking script not found")
					continue
				}

				if err := rejectTx.AddOutput(refundLockingScript, output.Value, false,
					false); err != nil {
					return nil, errors.Wrap(err, "add agent bitcoin transfer output")
				}

				outpoint := wire.OutPoint{
					Hash:  transaction.TxID(),
					Index: uint32(outputIndex),
				}
				if err := rejectTx.AddInput(outpoint, output.LockingScript,
					output.Value); err != nil {
					transaction.Unlock()
					return nil, errors.Wrap(err, "add agent bitcoin transfer input")
				}
			}
		}
	}

	inputFound := false
	if !inputFound && responseOutputIndex != -1 {
		outpoint := wire.OutPoint{
			Hash:  txid,
			Index: uint32(responseOutputIndex),
		}
		output := transaction.Output(responseOutputIndex)

		if !output.LockingScript.Equal(agentLockingScript) {
			return nil, errors.New("Reject output index locking script is wrong")
		}

		if err := rejectTx.AddInput(outpoint, output.LockingScript, output.Value); err != nil {
			transaction.Unlock()
			return nil, errors.Wrap(err, "add response input")
		}
		inputFound = true
	}

	if !inputFound && reject != nil && reject.OutputIndex != -1 {
		// Add input spending output flagged for response.
		outpoint := wire.OutPoint{
			Hash:  txid,
			Index: uint32(reject.OutputIndex),
		}
		output := transaction.Output(reject.OutputIndex)

		if !output.LockingScript.Equal(agentLockingScript) {
			return nil, errors.New("Reject output index locking script is wrong")
		}

		if err := rejectTx.AddInput(outpoint, output.LockingScript, output.Value); err != nil {
			transaction.Unlock()
			return nil, errors.Wrap(err, "add response input")
		}
		inputFound = true
	}

	if !inputFound {
		// Find any other outputs with the contract locking script.
		outputCount := transaction.OutputCount()
		for i := 0; i < outputCount; i++ {
			output := transaction.Output(i)
			if output.LockingScript.Equal(agentLockingScript) {
				outpoint := wire.OutPoint{
					Hash:  transaction.TxID(),
					Index: uint32(i),
				}

				if err := rejectTx.AddInput(outpoint, output.LockingScript,
					output.Value); err != nil && errors.Cause(err) != txbuilder.ErrDuplicateInput {
					transaction.Unlock()
					return nil, errors.Wrap(err, "add input")
				}
			}
		}
	}

	if len(rejectTx.MsgTx.TxIn) == 0 {
		transaction.Unlock()
		return nil, errors.New("No agent outputs for rejection")
	}

	if reject == nil || len(reject.ReceiverLockingScript) == 0 {
		// Add output with locking script that had the issue. This is referenced by the
		// RejectAddressIndex zero value of the rejection action.
		inputIndex := 0
		if reject != nil {
			inputIndex = reject.InputIndex
		}

		rejectInputOutput, err := transaction.InputOutput(inputIndex)
		if err != nil {
			transaction.Unlock()
			return nil, errors.Wrap(err, "reject input output")
		}

		if err := rejectTx.AddOutput(rejectInputOutput.LockingScript, 1, false, true); err != nil {
			transaction.Unlock()
			return nil, errors.Wrap(err, "add receiver")
		}
	} else {
		if err := rejectTx.AddOutput(reject.ReceiverLockingScript, 1, true, true); err != nil {
			transaction.Unlock()
			return nil, errors.Wrap(err, "add receiver")
		}
	}

	contractFee := a.ContractFee()
	if contractFee > 0 {
		if err := rejectTx.AddOutput(a.FeeLockingScript(), contractFee, false, false); err != nil {
			return nil, errors.Wrap(err, "add contract fee")
		}
	}

	// Add rejection action
	rejection := &actions.Rejection{
		Timestamp: a.Now(),
	}

	if reject != nil {
		rejection.RejectionCode = reject.Code
		rejection.Message = reject.Message
	} else {
		rejection.RejectionCode = actions.RejectionsMsgMalformed
		rejection.Message = rejectError.Error()
	}

	var rejectLabel string
	rejectData := actions.RejectionsData(rejection.RejectionCode)
	if rejectData != nil {
		rejectLabel = rejectData.Label
	} else {
		rejectLabel = "<unknown>"
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("request_txid", txid),
		logger.Uint32("reject_code", rejection.RejectionCode),
		logger.String("reject_label", rejectLabel),
		logger.String("reject_message", rejection.Message),
	}, "Responding with rejection")

	rejectionScript, err := protocol.Serialize(rejection, config.IsTest)
	if err != nil {
		transaction.Unlock()
		return nil, errors.Wrap(err, "serialize rejection")
	}

	rejectionScriptOutputIndex := len(rejectTx.Outputs)
	if err := rejectTx.AddOutput(rejectionScript, 0, false, false); err != nil {
		transaction.Unlock()
		return nil, errors.Wrap(err, "add action")
	}

	// Set locking script of largest input to receive any bitcoin change. Assume it is the main
	// funding and is the best option to receive any extra bitcoin.
	inputCount := transaction.InputCount()
	if reject == nil || len(reject.ReceiverLockingScript) == 0 {
		var refundInputValue uint64
		var refundScript bitcoin.Script
		for i := 0; i < inputCount; i++ {
			inputOutput, err := transaction.InputOutput(i)
			if err != nil {
				transaction.Unlock()
				return nil, errors.Wrapf(err, "input output %d", i)
			}

			if refundInputValue < inputOutput.Value {
				refundInputValue = inputOutput.Value
				refundScript = inputOutput.LockingScript
			}
		}

		if refundInputValue == 0 {
			if err := rejectTx.SetChangeLockingScript(changeLockingScript, ""); err != nil {
				return nil, errors.Wrap(err, "set change locking script")
			}
		} else {
			changeLockingScript = refundScript
			if err := rejectTx.SetChangeLockingScript(refundScript, ""); err != nil {
				return nil, errors.Wrap(err, "set change locking script")
			}
		}
	} else {
		changeLockingScript = reject.ReceiverLockingScript
		if err := rejectTx.SetChangeLockingScript(reject.ReceiverLockingScript, ""); err != nil {
			return nil, errors.Wrap(err, "set change locking script")
		}
	}
	transaction.Unlock()

	if err := a.Sign(ctx, rejectTx, changeLockingScript, agentBitcoinTransferUnlocker); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding for reject : %s", err)
			return nil, nil
		}

		return nil, errors.Wrap(err, "sign")
	}

	rejectTxID := *rejectTx.MsgTx.TxHash()
	rejectTransaction, err := a.transactions.AddRaw(ctx, rejectTx.MsgTx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, rejectTxID)

	// Set rejection tx as processed.
	rejectTransaction.Lock()
	rejectTransaction.SetProcessed(a.ContractHash(), rejectionScriptOutputIndex)
	rejectTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), actionOutputIndex, rejectTxID)
	tx := transaction.Tx.Copy()
	transaction.Unlock()

	etx, err := buildExpandedTx(rejectTx.MsgTx, []*wire.MsgTx{&tx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	if err := a.AddResponse(ctx, txid, nil, false, etx); err != nil {
		return etx, errors.Wrap(err, "respond")
	}

	return etx, nil
}
