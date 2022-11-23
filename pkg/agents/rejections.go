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
	"github.com/tokenized/specification/dist/golang/messages"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processRejection(ctx context.Context, transaction *state.Transaction,
	rejection *actions.Rejection, outputIndex int, now uint64) error {

	transaction.Lock()
	firstInput := transaction.Input(0)
	rejectedTxID := firstInput.PreviousOutPoint.Hash

	receiverIndex := int(rejection.RejectAddressIndex)
	outputCount := transaction.OutputCount()
	if outputCount <= receiverIndex {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Int("receiver_index", receiverIndex),
			logger.Int("output_count", outputCount),
		}, "Invalid rejection receiver index")

		transaction.Unlock()
		return nil
	}

	output := transaction.Output(receiverIndex)

	transaction.Unlock()

	agentLockingScript := a.LockingScript()
	if !output.LockingScript.Equal(agentLockingScript) {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("receiver_locking_script", output.LockingScript),
		}, "Agent is not the message receiver")
		return nil
	}

	// Check if this is a reject from another contract in a multi-contract transfer to a settlement
	// request.
	rejectedTransaction, err := a.caches.Transactions.Get(ctx, rejectedTxID)
	if err != nil {
		return errors.Wrap(err, "get tx")
	}

	if rejectedTransaction == nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("rejected_txid", rejectedTxID),
		}, "Rejected transaction not found")
		return nil
	}
	defer a.caches.Transactions.Release(ctx, rejectedTxID)

	// Find action
	isTest := a.IsTest()
	rejectedTransaction.Lock()
	outputCount = rejectedTransaction.OutputCount()
	var message *actions.Message
	for i := 0; i < outputCount; i++ {
		output := rejectedTransaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, isTest)
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
		return nil
	}

	// Verify this is from the next contract in the transfer.
	transferTransaction, transfer, transferOutputIndex, err := a.traceToTransfer(ctx,
		firstInputTxID(rejectedTransaction))
	if err != nil {
		return errors.Wrap(err, "trace to transfer")
	}

	if transferTransaction == nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("rejected_txid", rejectedTxID),
		}, "Rejected transaction does not trace to a transfer")
		return nil
	}
	transferTxID := transferTransaction.GetTxID()
	defer a.caches.Transactions.Release(ctx, transferTxID)

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("transfer_txid", transferTxID),
	}, "Found related transfer transaction")

	transferContracts, err := parseTransferContracts(transferTransaction, transfer,
		agentLockingScript, now)
	if err != nil {
		if errors.Cause(err) == ErrNotRelevant {
			logger.Warn(ctx, "Transfer not relevant to this contract agent")
			return nil // Not for this contract
		}

		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			if transferContracts != nil && transferContracts.FirstContractOutputIndex != -1 {
				rejectError.OutputIndex = transferContracts.FirstContractOutputIndex
			}
			if transferContracts != nil && transferContracts.IsFirstContract() {
				return errors.Wrap(a.sendRejection(ctx, transferTransaction, transferOutputIndex,
					rejectError, now), "reject")
			}

			return nil // Only first contract can reject at this point
		}

		return errors.Wrap(err, "parse contracts")
	}

	if transferContracts.IsFirstContract() {
		// This is the first contract so create a reject for the transfer itself.
		return errors.Wrap(a.sendRejection(ctx, transferTransaction, transferOutputIndex,
			platform.NewRejectErrorWithOutputIndex(int(rejection.RejectionCode), rejection.Message,
				transferContracts.FirstContractOutputIndex), now), "reject")
	}

	// This is not the first contract so create a reject to the previous contract agent.
	if len(transferContracts.PreviousLockingScript) == 0 {
		return errors.New("Previous locking script missing for send signature request")
	}

	return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
		platform.NewRejectErrorFull(int(rejection.RejectionCode),
			rejection.Message, 0, -1, transferContracts.PreviousLockingScript), now), "reject")
}

func firstInputTxID(transaction *state.Transaction) bitcoin.Hash32 {
	transaction.Lock()
	firstInput := transaction.Input(0)
	txid := firstInput.PreviousOutPoint.Hash
	transaction.Unlock()

	return txid
}

func (a *Agent) traceToTransfer(ctx context.Context,
	txid bitcoin.Hash32) (*state.Transaction, *actions.Transfer, int, error) {

	previousTransaction, err := a.caches.Transactions.Get(ctx, txid)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "get tx")
	}

	if previousTransaction != nil {
		// Find action
		isTest := a.IsTest()
		previousTransaction.Lock()
		firstInput := previousTransaction.Input(0)
		previousTxID := firstInput.PreviousOutPoint.Hash
		outputCount := previousTransaction.OutputCount()
		var message *actions.Message
		for i := 0; i < outputCount; i++ {
			output := previousTransaction.Output(i)
			action, err := protocol.Deserialize(output.LockingScript, isTest)
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
		a.caches.Transactions.Release(ctx, txid)

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
	isTest := a.IsTest()
	var message *actions.Message
	for _, txout := range fetchedTx.TxOut {
		act, err := protocol.Deserialize(txout.LockingScript, isTest)
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

// sendRejection creates a reject message transaction that spends the specified output and contains
// the specified reject code.
func (a *Agent) sendRejection(ctx context.Context, transaction *state.Transaction, outputIndex int,
	rejectError error, now uint64) error {

	var reject *platform.RejectError
	if re, ok := rejectError.(platform.RejectError); ok {
		reject = &re
	} else if re, ok := rejectError.(*platform.RejectError); ok {
		reject = re
	}

	agentLockingScript := a.LockingScript()
	rejectTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	transaction.Lock()

	if reject != nil && reject.OutputIndex != -1 {
		// Add input spending output flagged for response.
		outpoint := wire.OutPoint{
			Hash:  transaction.TxID(),
			Index: uint32(reject.OutputIndex),
		}
		output := transaction.Output(reject.OutputIndex)

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
		if reject != nil && i == reject.OutputIndex {
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
			return errors.Wrap(err, "reject input output")
		}

		if err := rejectTx.AddOutput(rejectInputOutput.LockingScript, 1, false, true); err != nil {
			transaction.Unlock()
			return errors.Wrap(err, "add receiver")
		}
	} else {
		if err := rejectTx.AddOutput(reject.ReceiverLockingScript, 1, true, true); err != nil {
			transaction.Unlock()
			return errors.Wrap(err, "add receiver")
		}
	}

	// Add rejection action
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
	if reject == nil || len(reject.ReceiverLockingScript) == 0 {
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
	} else {
		refundScript = reject.ReceiverLockingScript
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

	rejectTxID := *rejectTx.MsgTx.TxHash()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), outputIndex, rejectTxID)
	transaction.Unlock()

	var rejectLabel string
	rejectData := actions.RejectionsData(rejection.RejectionCode)
	if rejectData != nil {
		rejectLabel = rejectData.Label
	} else {
		rejectLabel = "<unknown>"
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("response_txid", rejectTxID),
		logger.Uint32("reject_code", rejection.RejectionCode),
		logger.String("reject_label", rejectLabel),
		logger.String("reject_message", rejection.Message),
	}, "Responding with rejection")
	if err := a.BroadcastTx(ctx, rejectTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	return nil
}
