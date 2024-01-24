package agents

import (
	"context"

	"github.com/tokenized/bitcoin_interpreter"
	"github.com/tokenized/bitcoin_interpreter/agent_bitcoin_transfer"
	"github.com/tokenized/bitcoin_interpreter/p2pkh"
	"github.com/tokenized/channels"
	"github.com/tokenized/envelope/pkg/golang/envelope/base"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/messages"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"
	"github.com/tokenized/txbuilder/fees"

	"github.com/pkg/errors"
)

func (a *Agent) processRejection(ctx context.Context, transaction *transactions.Transaction,
	rejection *actions.Rejection, actionIndex int) (*expanded_tx.ExpandedTx, error) {

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

	var otherReceivers []bitcoin.Script
	for _, addressIndex := range rejection.AddressIndexes {
		if addressIndex >= uint32(outputCount) {
			continue
		}

		otherOutput := transaction.Output(int(addressIndex))
		otherReceivers = append(otherReceivers, otherOutput.LockingScript.Copy())
	}

	transaction.Unlock()

	agentLockingScript := a.LockingScript()
	if agentLockingScript.Equal(inputOutput.LockingScript) {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("receiver_locking_script", output.LockingScript),
		}, "Agent is the rejection sender")
		if _, err := a.addResponseTxID(ctx, rejectedTxID, txid, rejection,
			actionIndex); err != nil {
			return nil, errors.Wrap(err, "add response txid")
		}

		transaction.Lock()
		transaction.SetProcessed(a.ContractHash(), actionIndex)
		transaction.Unlock()

		return nil, nil
	}

	if !output.LockingScript.Equal(agentLockingScript) {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("receiver_locking_script", output.LockingScript),
		}, "Agent is not the rejection receiver")

		isRelevant := false
		for _, otherReceiver := range otherReceivers {
			if otherReceiver.Equal(agentLockingScript) {
				logger.Info(ctx, "Agent is additional rejection receiver")
				isRelevant = true
				break
			}
		}

		if !isRelevant {
			return nil, nil
		}
	}

	// Check if this is a reject from another contract in a multi-contract transfer to a settlement
	// request.
	rejectedTransaction, err := a.transactions.GetTxWithAncestors(ctx, rejectedTxID)
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
	var transferTransaction *transactions.Transaction
	var transfer *actions.Transfer
	var transferOutputIndex int
	var transferTxID bitcoin.Hash32
	rejectIsTransfer := false
	for i := 0; i < outputCount; i++ {
		output := rejectedTransaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, config.IsTest)
		if err != nil {
			continue
		}

		switch act := action.(type) {
		case *actions.Message:
			if act.MessageCode == messages.CodeSettlementRequest ||
				act.MessageCode == messages.CodeSignatureRequest {
				message = act
				break
			}

		case *actions.Transfer:
			transferTransaction = rejectedTransaction
			transfer = act
			transferOutputIndex = i
			transferTxID = rejectedTransaction.TxID()
			rejectIsTransfer = true
			break
		}
	}
	rejectedTransaction.Unlock()

	if message != nil {
		// Verify this is from the next contract in the transfer.
		transferTransaction, transfer, transferOutputIndex, err = a.traceToTransfer(ctx,
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

		transferTxID = transferTransaction.GetTxID()
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("rejected_txid", rejectedTxID),
			logger.Stringer("transfer_txid", transferTxID),
		}, "Rejected transaction traced to a transfer")
		defer a.transactions.Release(ctx, transferTxID)
	} else if transfer == nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("rejected_txid", rejectedTxID),
		}, "Rejected transaction does not contain an appropriate action")
		return nil, nil
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("transfer_txid", transferTxID),
	}, "Found related transfer transaction")

	if err := a.cancelTransfer(ctx, transferTransaction, transfer); err != nil {
		return nil, errors.Wrap(err, "cancel transfer")
	}

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

	if rejectIsTransfer {
		// This is the first contract so create a reject for the transfer itself.
		etx, rerr := a.createRejection(ctx, transferTransaction, transferOutputIndex, -1,
			platform.NewRejectError(int(rejection.RejectionCode), rejection.Message))
		if rerr != nil {
			return nil, errors.Wrap(rerr, "reject transfer")
		}

		return etx, nil
	} else if transferContracts.IsFirstContract() {
		// This is the first contract so create a reject for the transfer itself.
		etx, rerr := a.createRejection(ctx, transferTransaction, transferOutputIndex, -1,
			platform.NewRejectErrorWithOutputIndex(int(rejection.RejectionCode), rejection.Message,
				transferContracts.FirstContractOutputIndex))
		if rerr != nil {
			return nil, errors.Wrap(rerr, "reject message")
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

	previousTransaction, err := a.transactions.GetTxWithAncestors(ctx, txid)
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

			switch act := action.(type) {
			case *actions.Transfer:
				if err := a.transactions.PopulateAncestors(ctx,
					previousTransaction); err != nil {
					previousTransaction.Unlock()
					a.transactions.Release(ctx, txid)
					return nil, nil, 0, errors.Wrap(err, "populate ancestors")
				}

				previousTransaction.Unlock()
				return previousTransaction, act, i, nil
			case *actions.Message:
				message = act
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

	config := a.Config()
	agentLockingScript := a.LockingScript()
	contractHash := state.CalculateContractHash(agentLockingScript)

	transaction.Lock()
	txid := transaction.TxID()
	processed := transaction.ContractProcessed(contractHash, actionOutputIndex)
	transaction.Unlock()
	for _, p := range processed {
		if p.ResponseTxID == nil {
			continue
		}

		responseTransaction, err := a.transactions.Get(ctx, *p.ResponseTxID)
		if err != nil {
			return nil, errors.Wrap(err, "get response transaction")
		}

		if responseTransaction == nil {
			continue
		}
		defer a.transactions.Release(ctx, *p.ResponseTxID)

		responseTransaction.Lock()
		responseOutputCount := responseTransaction.OutputCount()
		isComplete := false
		for i := 0; i < responseOutputCount && !isComplete; i++ {
			output := responseTransaction.Output(i)
			action, err := protocol.Deserialize(output.LockingScript, config.IsTest)
			if err != nil {
				continue
			}

			switch action.(type) {
			case *actions.Message:
			default: // anything but a message is complete
				isComplete = true
			}
		}
		responseTransaction.Unlock()

		if isComplete {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", agentLockingScript),
				logger.Stringer("txid", txid),
				logger.Int("action_index", actionOutputIndex),
				logger.Stringer("response_txid", processed[0].ResponseTxID),
			}, "Action already completed. Not generating rejection")

			return nil, nil
		}
	}

	var reject *platform.RejectError
	if re, ok := rejectError.(platform.RejectError); ok {
		reject = &re
	} else if re, ok := rejectError.(*platform.RejectError); ok {
		reject = re
	}

	changeLockingScript := a.FeeLockingScript()
	rejectTx := txbuilder.NewTxBuilder(float32(config.FeeRate), float32(config.DustFeeRate))

	transaction.Lock()
	outputCount := transaction.OutputCount()

	actionOutput := transaction.Output(actionOutputIndex)
	action, err := protocol.Deserialize(actionOutput.LockingScript, config.IsTest)
	if err != nil {
		transaction.Unlock()
		return nil, errors.Wrap(err, "deserialize action")
	}

	var agentBitcoinTransferUnlocker bitcoin_interpreter.Unlocker
	var refundOutputs []*wire.TxOut
	var otherContracts []bitcoin.Script
	var unallocatedRefund uint64
	if transfer, ok := action.(*actions.Transfer); ok {
		var bitcoinTransfer *actions.InstrumentTransferField
		var transferFeeTotal uint64
		transferFees := make([]uint64, len(transfer.Instruments))
		for instrumentIndex, instrumentTransfer := range transfer.Instruments {
			if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
				bitcoinTransfer = instrumentTransfer
				continue
			}

			if int(instrumentTransfer.ContractIndex) >= outputCount {
				continue
			}

			output := transaction.Output(int(instrumentTransfer.ContractIndex))
			if !output.LockingScript.Equal(agentLockingScript) {
				otherContracts = appendLockingScript(otherContracts, output.LockingScript)
				continue
			}

			var instrumentCode state.InstrumentCode
			copy(instrumentCode[:], instrumentTransfer.InstrumentCode)

			instrument, err := a.caches.Instruments.Get(ctx, agentLockingScript, instrumentCode)
			if err != nil {
				transaction.Unlock()
				return nil, errors.Wrap(err, "get instrument")
			}

			if instrument == nil {
				continue
			}

			instrument.Lock()
			if instrument.Creation != nil && instrument.Creation.TransferFee != nil {
				transferFeeTotal += instrument.Creation.TransferFee.Quantity
				transferFees[instrumentIndex] = instrument.Creation.TransferFee.Quantity
			}
			instrument.Unlock()
			a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)

			if transferFees[instrumentIndex] > 0 {
				allocated := false
				if len(instrumentTransfer.RefundAddress) > 0 {
					refundAddress, err := bitcoin.DecodeRawAddress(instrumentTransfer.RefundAddress)
					if err == nil {
						refundLockingScript, err := refundAddress.LockingScript()
						if err == nil {
							allocated = true
							refundOutputs = append(refundOutputs, &wire.TxOut{
								LockingScript: refundLockingScript,
								Value:         transferFees[instrumentIndex],
							})
						}
					}
				}

				if !allocated {
					unallocatedRefund += transferFees[instrumentIndex]
				}
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

						if info.RefundMatches(inputOutput.LockingScript, output.Value) {
							refundLockingScript = inputOutput.LockingScript
							break
						}
					}
				}

				if len(refundLockingScript) == 0 {
					for subOutputIndex := 0; subOutputIndex < outputCount; subOutputIndex++ {
						subTxout := transaction.Output(subOutputIndex)
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
					transaction.Unlock()
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
			transaction.Unlock()
			return nil, errors.New("Reject response output index locking script is wrong")
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
			transaction.Unlock()
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

	contractFeeOutputIndex := -1
	contractFee := a.ContractFee()
	if contractFee > 0 {
		contractFeeOutputIndex = len(rejectTx.MsgTx.TxOut)
		if err := rejectTx.AddOutput(a.FeeLockingScript(), contractFee, false, false); err != nil {
			transaction.Unlock()
			return nil, errors.Wrap(err, "add contract fee")
		}
	}

	// Add rejection action
	now := a.Now()
	rejection := &actions.Rejection{
		Timestamp: now,
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

	for _, otherContract := range otherContracts {
		rejection.AddressIndexes = append(rejection.AddressIndexes,
			uint32(len(rejectTx.MsgTx.TxOut)))
		if err := rejectTx.AddOutput(otherContract, 0, false, true); err != nil {
			transaction.Unlock()
			return nil, errors.Wrap(err, "add contract notify output")
		}
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
				transaction.Unlock()
				return nil, errors.Wrap(err, "set change locking script")
			}
		} else {
			changeLockingScript = refundScript
			if err := rejectTx.SetChangeLockingScript(refundScript, ""); err != nil {
				transaction.Unlock()
				return nil, errors.Wrap(err, "set change locking script")
			}
		}
	} else {
		changeLockingScript = reject.ReceiverLockingScript
		if err := rejectTx.SetChangeLockingScript(reject.ReceiverLockingScript, ""); err != nil {
			transaction.Unlock()
			return nil, errors.Wrap(err, "set change locking script")
		}
	}

	// Allocate any unallocated refund value. Split it, with weighting, between any non-dust inputs
	// in the request tx.
	if unallocatedRefund > 0 {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("value", unallocatedRefund),
		}, "Allocating refund to funding inputs")

		inputCount := transaction.InputCount()
		var fundingInputs []*wire.TxOut
		totalFunding := uint64(0)
		for inputIndex := 0; inputIndex < inputCount; inputIndex++ {
			inputOutput, err := transaction.InputOutput(inputIndex)
			if err != nil {
				transaction.Unlock()
				return nil, errors.Wrap(err, "input output")
			}

			dustLimit := fees.DustLimitForLockingScript(inputOutput.LockingScript,
				float64(rejectTx.DustFeeRate))

			if inputOutput.Value <= dustLimit {
				continue
			}

			fundingInputs = append(fundingInputs, inputOutput)
			totalFunding += inputOutput.Value
		}

		remainingValue := unallocatedRefund
		for index, fundingInput := range fundingInputs {
			var refundValue uint64
			if index == len(fundingInputs)-1 {
				refundValue = remainingValue
			} else {
				// Calculate percentage
				factor := float64(fundingInput.Value) / float64(totalFunding)
				refundValue = uint64(factor * float64(unallocatedRefund))
			}
			remainingValue -= refundValue

			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("locking_script", fundingInput.LockingScript),
				logger.Uint64("value", refundValue),
			}, "Allocated refund")
			refundOutputs = append(refundOutputs, &wire.TxOut{
				LockingScript: fundingInput.LockingScript,
				Value:         refundValue,
			})
		}
	}
	transaction.Unlock()

	// Add outputs if there is enough bitcoin.
	for _, refundOutput := range refundOutputs {
		outputFee := fees.OutputFeeForLockingScript(refundOutput.LockingScript,
			float64(rejectTx.FeeRate))
		estimatedFee := rejectTx.EstimatedFee()
		actualFee := rejectTx.ActualFee()
		if actualFee < 0 || uint64(actualFee) <= estimatedFee+outputFee {
			break
		}

		if uint64(actualFee) < estimatedFee+outputFee+refundOutput.Value {
			remainingValue := uint64(actualFee) - (estimatedFee + outputFee)
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("locking_script", refundOutput.LockingScript),
				logger.Uint64("value", remainingValue),
			}, "Adding remaining refund output")

			if err := rejectTx.AddOutput(refundOutput.LockingScript, remainingValue, true,
				false); err != nil {
				return nil, errors.Wrap(err, "add refund output")
			}

			break
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("locking_script", refundOutput.LockingScript),
			logger.Uint64("value", refundOutput.Value),
		}, "Adding refund output")

		if err := rejectTx.AddOutput(refundOutput.LockingScript, refundOutput.Value, false,
			false); err != nil {
			return nil, errors.Wrap(err, "add refund output")
		}
	}

	if err := a.Sign(ctx, rejectTx, changeLockingScript, agentBitcoinTransferUnlocker); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding for reject : %s", err)

			response := &channels.Response{
				Status:         channels.StatusReject,
				CodeProtocolID: base.ProtocolID(protocol.ProtocolID),
				Code:           actions.RejectionsInsufficientValue,
				Note:           err.Error(),
			}

			if err := a.AddTxIDResponse(ctx, txid, response); err != nil {
				return nil, errors.Wrap(err, "respond")
			}

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

	if err := a.updateRequestStats(ctx, &tx, rejectTx.MsgTx, actionOutputIndex,
		contractFeeOutputIndex, true, now); err != nil {
		logger.Error(ctx, "Failed to update statistics : %s", err)
	}

	return etx, nil
}
