package agents

import (
	"bytes"
	"context"
	"fmt"

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

func (a *Agent) processSignatureRequest(ctx context.Context, transaction *state.Transaction,
	outputIndex int, signatureRequest *messages.SignatureRequest, now uint64) error {

	agentLockingScript := a.LockingScript()

	// Deserialize payload transaction.
	tx := &wire.MsgTx{}
	if err := tx.Deserialize(bytes.NewReader(signatureRequest.Payload)); err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error()), now), "reject")
	}

	if len(tx.TxIn) == 0 {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "settlement missing inputs"),
			now), "reject")
	}

	// Parse settlement action.
	isTest := a.IsTest()
	settlementScriptOutputIndex := 0
	var settlement *actions.Settlement
	for i, txout := range tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, isTest)
		if err != nil {
			continue
		}

		if s, ok := action.(*actions.Settlement); ok {
			if settlement != nil {
				return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
					platform.NewRejectError(actions.RejectionsMsgMalformed,
						"more than on settlement"), now), "reject")
			}
			settlementScriptOutputIndex = i
			settlement = s
		} else {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("non settlement action: %s", action.Code())), now), "reject")
		}
	}

	if settlement == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "missing settlement action"),
			now), "reject")
	}

	// Add spent outputs to transaction.
	transferTxID := tx.TxIn[0].PreviousOutPoint.Hash
	for _, txin := range tx.TxIn[1:] {
		if !transferTxID.Equal(&txin.PreviousOutPoint.Hash) {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					"settlement inputs not all from transfer tx"), now), "reject")
		}
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("transfer_txid", transferTxID),
	}, "TransferTxID")

	transferTransaction, err := a.caches.Transactions.Get(ctx, transferTxID)
	if err != nil {
		return errors.Wrap(err, "get transfer tx")
	}

	if transferTransaction == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "unknown transfer tx"), now),
			"reject")
	}
	defer a.caches.Transactions.Release(ctx, transferTxID)

	transferTransaction.Lock()
	transferTx := transferTransaction.GetMsgTx()
	transferOutputIndex := 0
	var transfer *actions.Transfer
	for i, txout := range transferTx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, isTest)
		if err != nil {
			continue
		}

		if t, ok := action.(*actions.Transfer); ok {
			transferOutputIndex = i
			transfer = t
			break
		}
	}

	if transfer == nil {
		transferTransaction.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "missing transfer action"),
			now), "reject")
	}
	transferTransaction.Unlock()

	transferContracts, err := parseTransferContracts(transferTransaction, transfer,
		agentLockingScript, now)
	if err != nil {
		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex, rejectError, now),
				"reject")
		}

		return errors.Wrap(err, "parse contracts")
	}

	rejectTransaction := transaction
	rejectOutputIndex := 0
	rejectActionOutputIndex := 0
	var rejectLockingScript bitcoin.Script
	if transferContracts.IsFirstContract() {
		rejectTransaction = transferTransaction
		rejectOutputIndex = transferContracts.FirstContractOutputIndex
		rejectActionOutputIndex = transferOutputIndex
	} else {
		rejectLockingScript = transferContracts.PreviousLockingScript
		rejectActionOutputIndex = outputIndex
	}

	transaction.Lock()
	firstInputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrap(err, "get first input output")
	}

	if !firstInputOutput.LockingScript.Equal(transferContracts.NextLockingScript) {
		return errors.Wrap(a.sendRejection(ctx, rejectTransaction, rejectActionOutputIndex,
			platform.NewRejectErrorFull(actions.RejectionsMsgMalformed,
				"signature request not from next contract", 0, rejectOutputIndex,
				rejectLockingScript), now), "reject")
	}

	if err := a.CheckContractIsAvailable(now); err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex, err, now), "reject")
	}

	settlementTx, err := txbuilder.NewTxBuilderFromWire(a.FeeRate(), a.DustFeeRate(), tx,
		[]*wire.MsgTx{transferTx})
	if err != nil {
		return errors.Wrap(err, "build settlement tx")
	}

	// Verify the tx has correct settlements for this contract.
	var balances state.Balances
	for index, instrumentSettlement := range settlement.Instruments {
		if instrumentSettlement.InstrumentType == protocol.BSVInstrumentID {
			instrumentCtx := logger.ContextWithLogFields(ctx,
				logger.String("instrument_id", protocol.BSVInstrumentID))

			if err := a.verifyBitcoinSettlement(instrumentCtx, transferTransaction, transfer,
				settlementTx, instrumentSettlement); err != nil {
				balances.Unlock()

				if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
					rejectError.OutputIndex = rejectOutputIndex
					rejectError.ReceiverLockingScript = rejectLockingScript
					return errors.Wrap(a.sendRejection(instrumentCtx, rejectTransaction,
						rejectActionOutputIndex, rejectError, now), "reject")
				}

				return errors.Wrapf(err, "verify settlement: %s", protocol.BSVInstrumentID)
			}
		}

		if !agentLockingScript.Equal(transferContracts.Outputs[index].LockingScript) {
			continue
		}

		instrumentCtx := ctx
		instrumentID, err := protocol.InstrumentIDForSettlement(instrumentSettlement)
		if err == nil {
			instrumentCtx = logger.ContextWithLogFields(instrumentCtx,
				logger.String("instrument_id", instrumentID))
		}

		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], instrumentSettlement.InstrumentCode)

		instrumentBalances, err := a.verifyInstrumentSettlement(instrumentCtx, agentLockingScript,
			instrumentCode, transferTxID, settlementTx, instrumentSettlement, now)
		if err != nil {
			balances.Unlock()

			if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
				rejectError.OutputIndex = rejectOutputIndex
				rejectError.ReceiverLockingScript = rejectLockingScript
				return errors.Wrap(a.sendRejection(instrumentCtx, rejectTransaction,
					rejectActionOutputIndex, rejectError, now), "reject")
			}

			return errors.Wrapf(err, "verify settlement: %s", instrumentID)
		}

		defer a.caches.Balances.ReleaseMulti(instrumentCtx, agentLockingScript, instrumentCode,
			instrumentBalances)
		balances = state.AppendBalances(balances, instrumentBalances)
	}

	// Verify exchange fee
	if transfer.ExchangeFee > 0 {
		ra, err := bitcoin.DecodeRawAddress(transfer.ExchangeFeeAddress)
		if err != nil {
			balances.Unlock()

			logger.Warn(ctx, "Invalid exchange fee address : %s", err)
			return errors.Wrap(a.sendRejection(ctx, rejectTransaction, rejectActionOutputIndex,
				platform.NewRejectErrorFull(actions.RejectionsMsgMalformed, err.Error(),
					0, rejectOutputIndex, rejectLockingScript), now), "reject")
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			balances.Unlock()

			logger.Warn(ctx, "Invalid exchange fee locking script : %s", err)
			return errors.Wrap(a.sendRejection(ctx, rejectTransaction, rejectActionOutputIndex,
				platform.NewRejectErrorFull(actions.RejectionsMsgMalformed, err.Error(),
					0, rejectOutputIndex, rejectLockingScript), now), "reject")
		}

		if !findBitcoinOutput(settlementTx.MsgTx, lockingScript, transfer.ExchangeFee) {
			balances.Unlock()

			return errors.Wrap(a.sendRejection(ctx, rejectTransaction, rejectActionOutputIndex,
				platform.NewRejectErrorFull(actions.RejectionsMsgMalformed,
					"missing exchange fee output", 0, rejectOutputIndex, rejectLockingScript), now),
				"reject")
		}
	}

	// Verify contract fee
	contractFee := a.ContractFee()
	if contractFee > 0 {
		if !findBitcoinOutput(settlementTx.MsgTx, a.FeeLockingScript(), contractFee) {
			balances.Unlock()

			return errors.Wrap(a.sendRejection(ctx, rejectTransaction, rejectActionOutputIndex,
				platform.NewRejectErrorFull(actions.RejectionsMsgMalformed,
					"missing contract fee output", 0, rejectOutputIndex, rejectLockingScript), now),
				"reject")
		}
	}

	// Sign settlement tx.
	if _, err := settlementTx.SignOnly([]bitcoin.Key{a.Key()}); err != nil {
		balances.Unlock()

		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, rejectTransaction, rejectActionOutputIndex,
				platform.NewRejectErrorFull(actions.RejectionsInsufficientTxFeeFunding,
					err.Error(), 0, rejectOutputIndex, rejectLockingScript), now), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	// If this is the first contract then ensure settlement tx is complete and broadcast.
	if transferContracts.IsFirstContract() {
		if err := a.completeSettlement(ctx, transferTransaction, transferOutputIndex, transferTxID,
			settlementTx, settlementScriptOutputIndex, balances, now); err != nil {
			balances.Unlock()
			return errors.Wrap(err, "complete settlement")
		}

		balances.Unlock()

		// Cancel scheduled task to cancel the transfer if other contract(s) don't respond.
		if a.scheduler != nil {
			a.scheduler.Cancel(ctx, transferTxID)
		}

		return nil
	}

	balances.Unlock()

	// If this isn't the first contract then create a signature request to the previous contract.
	if err := a.sendSignatureRequest(ctx, transaction, outputIndex, transferContracts, settlementTx,
		now); err != nil {
		return errors.Wrap(err, "send signature request")
	}

	// Cancel scheduled task to cancel the transfer if other contract(s) don't respond.
	if a.scheduler != nil {
		a.scheduler.Cancel(ctx, transferTxID)
	}

	return nil
}

func (a *Agent) verifyBitcoinSettlement(ctx context.Context, transferTransaction *state.Transaction,
	transfer *actions.Transfer, settlementTx *txbuilder.TxBuilder,
	instrumentSettlement *actions.InstrumentSettlementField) error {

	var instrumentTransfer *actions.InstrumentTransferField
	for _, inst := range transfer.Instruments {
		if inst.InstrumentType == protocol.BSVInstrumentID {
			instrumentTransfer = inst
			break
		}
	}

	if instrumentTransfer == nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed, "missing bitcoin settlement")
	}

	transferTransaction.Lock()
	defer transferTransaction.Unlock()

	quantity := uint64(0)
	var usedInputs []uint32
	for _, sender := range instrumentTransfer.InstrumentSenders {
		for _, used := range usedInputs {
			if used == sender.Index {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					"input used as bitcoin sender more than once")
			}
		}
		usedInputs = append(usedInputs, sender.Index)

		output, err := transferTransaction.InputOutput(int(sender.Index))
		if err != nil {
			return platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
		}

		if sender.Quantity >= output.Value {
			return platform.NewRejectError(actions.RejectionsInsufficientValue,
				"sender input value less than quantity")
		}

		quantity += output.Value
	}

	for i, receiver := range instrumentTransfer.InstrumentReceivers {
		ra, err := bitcoin.DecodeRawAddress(receiver.Address)
		if err != nil {
			return platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			return errors.Wrapf(err, "locking script %d", i)
		}

		if !findBitcoinOutput(settlementTx.MsgTx, lockingScript, receiver.Quantity) {
			return platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("missing bitcoin output %d", i))
		}

		if receiver.Quantity > quantity {
			return platform.NewRejectError(actions.RejectionsInsufficientValue,
				"sender quantity less than receiver")
		}

		quantity -= receiver.Quantity
	}

	if quantity != 0 {
		return platform.NewRejectError(actions.RejectionsInsufficientValue,
			"sender quantity more than receiver")
	}

	logger.Info(ctx, "Verified bitcoin transfer")
	return nil
}

func (a *Agent) verifyInstrumentSettlement(ctx context.Context, agentLockingScript bitcoin.Script,
	instrumentCode state.InstrumentCode, transferTxID bitcoin.Hash32,
	settlementTx *txbuilder.TxBuilder, instrumentSettlement *actions.InstrumentSettlementField,
	now uint64) (state.Balances, error) {

	var lockingScripts []bitcoin.Script
	outputCount := uint32(len(settlementTx.MsgTx.TxOut))
	for _, settlement := range instrumentSettlement.Settlements {
		if settlement.Index >= outputCount {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				"invalid settlement index")
		}

		txout := settlementTx.MsgTx.TxOut[settlement.Index]
		lockingScripts = appendLockingScript(lockingScripts, txout.LockingScript)
	}

	balances, err := a.caches.Balances.GetMulti(ctx, agentLockingScript, instrumentCode, lockingScripts)
	if err != nil {
		return nil, errors.Wrap(err, "get balances")
	}

	balances.Lock()

	for i, settlement := range instrumentSettlement.Settlements {
		txout := settlementTx.MsgTx.TxOut[settlement.Index]

		balance := balances.Find(txout.LockingScript)
		if balance == nil {
			balances.Unlock()
			a.caches.Balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Int("index", i),
				logger.Stringer("locking_script", txout.LockingScript),
			}, "Missing settlement balance")
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("missing settlement balance %d", i))
		}

		if rejectCode := balance.VerifySettlement(transferTxID,
			settlement.Quantity, now); rejectCode != 0 {
			balances.Unlock()
			a.caches.Balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)
			return nil, platform.NewRejectError(rejectCode, fmt.Sprintf("settlement %d", i))
		}
	}

	return balances, nil
}

func (a *Agent) sendSignatureRequest(ctx context.Context, currentTransaction *state.Transaction,
	currentOutputIndex int, transferContracts *TransferContracts, settlementTx *txbuilder.TxBuilder,
	now uint64) error {

	agentLockingScript := a.LockingScript()
	currentTxID := currentTransaction.GetTxID()

	if len(transferContracts.PreviousLockingScript) == 0 {
		return errors.New("Previous locking script missing for send signature request")
	}

	fundingIndex := 0
	currentTransaction.Lock()
	fundingOutput := currentTransaction.Output(int(fundingIndex))
	currentTransaction.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("funding_index", fundingIndex),
		logger.Uint64("funding_value", fundingOutput.Value),
	}, "Funding signature request with output")

	if !fundingOutput.LockingScript.Equal(agentLockingScript) {
		return fmt.Errorf("Wrong locking script for funding output")
	}

	messageTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := messageTx.AddInput(wire.OutPoint{Hash: currentTxID, Index: uint32(fundingIndex)},
		agentLockingScript, fundingOutput.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	if err := messageTx.AddOutput(transferContracts.PreviousLockingScript, 0, true,
		true); err != nil {
		return errors.Wrap(err, "add previous contract output")
	}

	settlementTxBuf := &bytes.Buffer{}
	if err := settlementTx.MsgTx.Serialize(settlementTxBuf); err != nil {
		return errors.Wrap(err, "serialize settlement tx")
	}

	signatureRequest := &messages.SignatureRequest{
		Timestamp: now,
		Payload:   settlementTxBuf.Bytes(),
	}

	payloadBuffer := &bytes.Buffer{}
	if err := signatureRequest.Serialize(payloadBuffer); err != nil {
		return errors.Wrap(err, "serialize signature request")
	}

	message := &actions.Message{
		ReceiverIndexes: []uint32{0}, // First output is receiver of message
		MessageCode:     signatureRequest.Code(),
		MessagePayload:  payloadBuffer.Bytes(),
	}

	messageScript, err := protocol.Serialize(message, a.IsTest())
	if err != nil {
		return errors.Wrap(err, "serialize message")
	}

	messageScriptOutputIndex := len(messageTx.Outputs)
	if err := messageTx.AddOutput(messageScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add message output")
	}

	if _, err := messageTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, currentTransaction, currentOutputIndex,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error()),
				now), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	messageTxID := *messageTx.MsgTx.TxHash()
	messageTransaction, err := a.caches.Transactions.AddRaw(ctx, messageTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, messageTxID)

	messageTransaction.Lock()
	messageTransaction.SetProcessed(a.ContractHash(), messageScriptOutputIndex)
	messageTransaction.Unlock()

	currentTransaction.Lock()
	currentTransaction.AddResponseTxID(a.ContractHash(), currentOutputIndex, messageTxID)
	currentTransaction.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("previous_contract_locking_script",
			transferContracts.PreviousLockingScript),
		logger.Stringer("response_txid", messageTxID),
	}, "Sending signature request to previous contract")
	if err := a.BroadcastTx(ctx, messageTx.MsgTx, message.ReceiverIndexes); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	return nil
}
