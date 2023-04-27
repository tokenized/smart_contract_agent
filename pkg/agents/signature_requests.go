package agents

import (
	"bytes"
	"context"
	"fmt"

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

	"github.com/pkg/errors"
)

func (a *Agent) processSignatureRequest(ctx context.Context, transaction *transactions.Transaction,
	outputIndex int, signatureRequest *messages.SignatureRequest,
	senderLockingScript, senderUnlockingScript bitcoin.Script) (*expanded_tx.ExpandedTx, error) {

	agentLockingScript := a.LockingScript()
	now := a.Now()

	// Deserialize payload transaction.
	tx := &wire.MsgTx{}
	if err := tx.Deserialize(bytes.NewReader(signatureRequest.Payload)); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	if len(tx.TxIn) == 0 {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"settlement missing inputs")
	}

	// Parse settlement action.
	config := a.Config()
	settlementScriptOutputIndex := 0
	var settlement *actions.Settlement
	for i, txout := range tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, config.IsTest)
		if err != nil {
			continue
		}

		if s, ok := action.(*actions.Settlement); ok {
			if settlement != nil {
				return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
					"more than on settlement")
			}
			settlementScriptOutputIndex = i
			settlement = s
		} else {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("non settlement action: %s", action.Code()))
		}
	}

	if settlement == nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"missing settlement action")
	}

	// Add spent outputs to transaction.
	transferTxID := tx.TxIn[0].PreviousOutPoint.Hash
	for _, txin := range tx.TxIn[1:] {
		if !transferTxID.Equal(&txin.PreviousOutPoint.Hash) {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				"settlement inputs not all from transfer tx")
		}
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("transfer_txid", transferTxID),
	}, "TransferTxID")

	transferTransaction, err := a.transactions.Get(ctx, transferTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get transfer tx")
	}

	if transferTransaction == nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "unknown transfer tx")
	}
	defer a.transactions.Release(ctx, transferTxID)

	transferTransaction.Lock()
	transferTx := transferTransaction.GetMsgTx()
	transferOutputIndex := 0
	var transfer *actions.Transfer
	for i, txout := range transferTx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, config.IsTest)
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
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"missing transfer action")
	}
	transferTransaction.Unlock()

	transferContracts, err := parseTransferContracts(transferTransaction, transfer,
		agentLockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "parse contracts")
	}

	if transferContracts.IsFirstContract() {
		processeds := transferTransaction.ContractProcessed(a.ContractHash(), transferOutputIndex)
		for _, processed := range processeds {
			if processed.ResponseTxID == nil {
				continue
			}

			// Check if response tx spends main transfer output and is the final response to the
			// transfer or if it just spends the boomerang output and is just an inter-contract
			// response.
			responseTransaction, err := a.transactions.Get(ctx, *processed.ResponseTxID)
			if err != nil {
				return nil, errors.Wrap(err, "get tx")
			}

			if responseTransaction == nil {
				return nil, errors.New("Transaction not found")
			}
			defer a.transactions.Release(ctx, *processed.ResponseTxID)

			isFinalResponse := false
			responseTransaction.Lock()
			inputCount := responseTransaction.InputCount()
			for i := 0; i < inputCount; i++ {
				txin := responseTransaction.Input(i)
				if txin.PreviousOutPoint.Hash.Equal(&transferTxID) &&
					transferContracts.IsFinalResponseOutput(int(txin.PreviousOutPoint.Index)) {
					isFinalResponse = true
					break
				}
			}
			responseTransaction.Unlock()

			if !isFinalResponse {
				continue
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", agentLockingScript),
				logger.Stringer("txid", transferTxID),
				logger.Int("output_index", transferOutputIndex),
				logger.String("action_code", transfer.Code()),
				logger.String("action_name", transfer.TypeName()),
				logger.Stringer("response_txid", processed.ResponseTxID),
			}, "Action already processed")
			return nil, nil
		}
	}

	rejectOutputIndex := -1
	var rejectLockingScript bitcoin.Script
	if transferContracts.IsFirstContract() {
		rejectOutputIndex = transferContracts.CurrentOutputIndex()
	} else {
		rejectLockingScript = transferContracts.PreviousLockingScript
	}

	if !senderLockingScript.Equal(transferContracts.NextLockingScript) {
		return nil, platform.NewRejectErrorFull(actions.RejectionsMsgMalformed,
			"signature request not from next contract", 0, rejectOutputIndex, rejectLockingScript)
	}

	if isSigHashAll, err := senderUnlockingScript.IsSigHashAll(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, err.Error())
	} else if !isSigHashAll {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, "")
	}

	if err := a.CheckContractIsAvailable(now); err != nil {
		return nil, platform.NewDefaultRejectError(err)
	}

	settlementTx, err := txbuilder.NewTxBuilderFromWire(config.FeeRate, config.DustFeeRate, tx,
		[]*wire.MsgTx{transferTx})
	if err != nil {
		return nil, errors.Wrap(err, "build settlement tx")
	}

	// Verify the tx has correct settlements for this contract.
	allBalances := make(state.BalanceSet, len(settlement.Instruments))
	instrumentCodes := make([]state.InstrumentCode, len(settlement.Instruments))
	for index, instrumentSettlement := range settlement.Instruments {
		instrumentID, _ := protocol.InstrumentIDForSettlement(instrumentSettlement)
		instrumentCtx := logger.ContextWithLogFields(ctx,
			logger.String("instrument_id", instrumentID))

		if instrumentSettlement.InstrumentType == protocol.BSVInstrumentID {
			continue
		}

		if !agentLockingScript.Equal(transferContracts.Outputs[index].LockingScript) {
			continue
		}

		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], instrumentSettlement.InstrumentCode)

		balances, err := a.fetchInstrumentSettlementBalances(instrumentCtx,
			agentLockingScript, instrumentCode, settlementTx, instrumentSettlement)
		if err != nil {
			return nil, errors.Wrap(err, "fetch instrument balances")
		}

		allBalances[index] = balances
		instrumentCodes[index] = instrumentCode

		defer a.caches.Balances.ReleaseMulti(instrumentCtx, agentLockingScript, instrumentCode,
			balances)
	}

	lockerResponseChannel := a.locker.AddRequest(allBalances)
	lockerResponse := <-lockerResponseChannel
	switch v := lockerResponse.(type) {
	case uint64:
		now = v
	case error:
		return nil, errors.Wrap(v, "locker")
	}
	defer allBalances.Unlock()

	for index, instrumentSettlement := range settlement.Instruments {
		instrumentID, _ := protocol.InstrumentIDForSettlement(instrumentSettlement)
		instrumentCtx := logger.ContextWithLogFields(ctx,
			logger.String("instrument_id", instrumentID))

		if instrumentSettlement.InstrumentType == protocol.BSVInstrumentID {
			if err := a.verifyBitcoinSettlement(instrumentCtx, transferTransaction, transfer,
				settlementTx, instrumentSettlement); err != nil {
				return nil, errors.Wrapf(err, "verify settlement: %s", protocol.BSVInstrumentID)
			}
		}

		if !agentLockingScript.Equal(transferContracts.Outputs[index].LockingScript) {
			continue
		}

		if err := a.verifyInstrumentSettlement(instrumentCtx, agentLockingScript,
			instrumentCodes[index], settlementTx, allBalances[index], transferTxID,
			instrumentSettlement, now); err != nil {
			return nil, errors.Wrapf(err, "verify settlement: %s", instrumentID)
		}
	}

	// Verify exchange fee
	if transfer.ExchangeFee > 0 {
		ra, err := bitcoin.DecodeRawAddress(transfer.ExchangeFeeAddress)
		if err != nil {
			allBalances.Revert(transferTxID)

			logger.Warn(ctx, "Invalid exchange fee address : %s", err)
			return nil, platform.NewRejectErrorFull(actions.RejectionsMsgMalformed, err.Error(), 0,
				rejectOutputIndex, rejectLockingScript)
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			allBalances.Revert(transferTxID)

			logger.Warn(ctx, "Invalid exchange fee locking script : %s", err)
			return nil, platform.NewRejectErrorFull(actions.RejectionsMsgMalformed, err.Error(), 0,
				rejectOutputIndex, rejectLockingScript)
		}

		if !findBitcoinOutput(settlementTx.MsgTx, lockingScript, transfer.ExchangeFee) {
			allBalances.Revert(transferTxID)

			return nil, platform.NewRejectErrorFull(actions.RejectionsMsgMalformed,
				"missing exchange fee output", 0, rejectOutputIndex, rejectLockingScript)
		}
	}

	// Verify contract fee
	contractFee := a.ContractFee()
	if contractFee > 0 {
		if !findBitcoinOutput(settlementTx.MsgTx, a.FeeLockingScript(), contractFee) {
			allBalances.Revert(transferTxID)

			return nil, platform.NewRejectErrorFull(actions.RejectionsMsgMalformed,
				"missing contract fee output", 0, rejectOutputIndex, rejectLockingScript)
		}
	}

	// Sign settlement tx.
	if _, err := settlementTx.SignOnly([]bitcoin.Key{a.Key()}); err != nil {
		allBalances.Revert(transferTxID)

		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return nil, platform.NewRejectErrorFull(actions.RejectionsInsufficientTxFeeFunding,
				err.Error(), 0, rejectOutputIndex, rejectLockingScript)
		}

		return nil, errors.Wrap(err, "sign")
	}

	// If this is the first contract then ensure settlement tx is complete and broadcast.
	if transferContracts.IsFirstContract() {
		etx, err := a.completeSettlement(ctx, transferTransaction, transferOutputIndex,
			transferTxID, settlementTx, settlementScriptOutputIndex, allBalances, now)
		if err != nil {
			allBalances.Revert(transferTxID)
			return etx, errors.Wrap(err, "complete settlement")
		}

		allBalances.Revert(transferTxID)

		// Cancel scheduled task to cancel the transfer if other contract(s) don't respond.
		if a.scheduler != nil {
			a.scheduler.Cancel(ctx, transferTxID)
		}

		return etx, nil
	}

	// If this isn't the first contract then create a signature request to the previous contract.
	etx, err := a.createSignatureRequest(ctx, transaction, outputIndex, transferContracts,
		settlementTx, now)
	if err != nil {
		return nil, errors.Wrap(err, "send signature request")
	}

	// Cancel scheduled task to cancel the transfer if other contract(s) don't respond.
	if a.scheduler != nil {
		a.scheduler.Cancel(ctx, transferTxID)
	}

	return etx, nil
}

func (a *Agent) verifyBitcoinSettlement(ctx context.Context, transferTransaction *transactions.Transaction,
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

func (a *Agent) fetchInstrumentSettlementBalances(ctx context.Context,
	agentLockingScript bitcoin.Script, instrumentCode state.InstrumentCode,
	settlementTx *txbuilder.TxBuilder,
	instrumentSettlement *actions.InstrumentSettlementField) (state.Balances, error) {

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

	balances, err := a.caches.Balances.GetMulti(ctx, agentLockingScript, instrumentCode,
		lockingScripts)
	if err != nil {
		return nil, errors.Wrap(err, "get balances")
	}

	return balances, nil
}

func (a *Agent) verifyInstrumentSettlement(ctx context.Context, agentLockingScript bitcoin.Script,
	instrumentCode state.InstrumentCode, settlementTx *txbuilder.TxBuilder, balances state.Balances,
	transferTxID bitcoin.Hash32, instrumentSettlement *actions.InstrumentSettlementField,
	now uint64) error {

	for i, settlement := range instrumentSettlement.Settlements {
		txout := settlementTx.MsgTx.TxOut[settlement.Index]

		balance := balances.Find(txout.LockingScript)
		if balance == nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Int("index", i),
				logger.Stringer("locking_script", txout.LockingScript),
			}, "Missing settlement balance")
			return platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("missing settlement balance %d", i))
		}

		if rejectCode := balance.VerifySettlement(transferTxID,
			settlement.Quantity, now); rejectCode != 0 {
			return platform.NewRejectError(rejectCode, fmt.Sprintf("settlement %d", i))
		}
	}

	return nil
}

func (a *Agent) createSignatureRequest(ctx context.Context,
	currentTransaction *transactions.Transaction, currentOutputIndex int,
	transferContracts *TransferContracts, settlementTx *txbuilder.TxBuilder,
	now uint64) (*expanded_tx.ExpandedTx, error) {

	agentLockingScript := a.LockingScript()
	currentTxID := currentTransaction.GetTxID()

	if len(transferContracts.PreviousLockingScript) == 0 {
		return nil, errors.New("Previous locking script missing for send signature request")
	}

	fundingIndex := 0
	currentTransaction.Lock()
	fundingOutput := currentTransaction.Output(int(fundingIndex))
	currentTransaction.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("funding_index", fundingIndex),
		logger.Uint64("funding_value", fundingOutput.Value),
	}, "Sending signature request")

	if !fundingOutput.LockingScript.Equal(agentLockingScript) {
		return nil, fmt.Errorf("Wrong locking script for funding output")
	}

	config := a.Config()
	messageTx := txbuilder.NewTxBuilder(config.FeeRate, config.DustFeeRate)

	if err := messageTx.AddInput(wire.OutPoint{Hash: currentTxID, Index: uint32(fundingIndex)},
		agentLockingScript, fundingOutput.Value); err != nil {
		return nil, errors.Wrap(err, "add input")
	}

	if err := messageTx.AddOutput(transferContracts.PreviousLockingScript, 0, true,
		true); err != nil {
		return nil, errors.Wrap(err, "add previous contract output")
	}

	settlementTxBuf := &bytes.Buffer{}
	if err := settlementTx.MsgTx.Serialize(settlementTxBuf); err != nil {
		return nil, errors.Wrap(err, "serialize settlement tx")
	}

	signatureRequest := &messages.SignatureRequest{
		Timestamp: now,
		Payload:   settlementTxBuf.Bytes(),
	}

	payloadBuffer := &bytes.Buffer{}
	if err := signatureRequest.Serialize(payloadBuffer); err != nil {
		return nil, errors.Wrap(err, "serialize signature request")
	}

	message := &actions.Message{
		ReceiverIndexes: []uint32{0}, // First output is receiver of message
		MessageCode:     signatureRequest.Code(),
		MessagePayload:  payloadBuffer.Bytes(),
	}

	messageScript, err := protocol.Serialize(message, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize message")
	}

	messageScriptOutputIndex := len(messageTx.Outputs)
	if err := messageTx.AddOutput(messageScript, 0, false, false); err != nil {
		return nil, errors.Wrap(err, "add message output")
	}

	if _, err := messageTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			return nil, platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding,
				err.Error())
		}

		return nil, errors.Wrap(err, "sign")
	}

	messageTxID := *messageTx.MsgTx.TxHash()
	messageTransaction, err := a.transactions.AddRaw(ctx, messageTx.MsgTx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, messageTxID)

	messageTransaction.Lock()
	messageTransaction.SetProcessed(a.ContractHash(), messageScriptOutputIndex)
	messageTransaction.Unlock()

	currentTransaction.Lock()
	currentTransaction.AddResponseTxID(a.ContractHash(), currentOutputIndex, messageTxID)
	currentTx := currentTransaction.Tx.Copy()
	currentTransaction.Unlock()

	etx, err := buildExpandedTx(messageTx.MsgTx, []*wire.MsgTx{currentTx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("previous_contract_locking_script",
			transferContracts.PreviousLockingScript),
		logger.Stringer("response_txid", messageTxID),
	}, "Sending signature request to previous contract")

	return etx, nil
}

func (a *Agent) createSignatureRequestRejection(ctx context.Context,
	transaction *transactions.Transaction, outputIndex int,
	signatureRequest *messages.SignatureRequest,
	rejectError platform.RejectError) (*expanded_tx.ExpandedTx, error) {

	agentLockingScript := a.LockingScript()

	// Deserialize payload transaction.
	tx := &wire.MsgTx{}
	if err := tx.Deserialize(bytes.NewReader(signatureRequest.Payload)); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	if len(tx.TxIn) == 0 {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"settlement missing inputs")
	}

	// Add spent outputs to transaction.
	transferTxID := tx.TxIn[0].PreviousOutPoint.Hash
	for _, txin := range tx.TxIn[1:] {
		if !transferTxID.Equal(&txin.PreviousOutPoint.Hash) {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				"settlement inputs not all from transfer tx")
		}
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("transfer_txid", transferTxID),
	}, "TransferTxID")

	transferTransaction, err := a.transactions.Get(ctx, transferTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get transfer tx")
	}

	if transferTransaction == nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "unknown transfer tx")
	}
	defer a.transactions.Release(ctx, transferTxID)

	config := a.Config()
	transferTransaction.Lock()
	transferTx := transferTransaction.GetMsgTx()
	transferOutputIndex := 0
	var transfer *actions.Transfer
	for i, txout := range transferTx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, config.IsTest)
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
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"missing transfer action")
	}
	transferTransaction.Unlock()

	transferContracts, err := parseTransferContracts(transferTransaction, transfer,
		agentLockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "parse contracts")
	}

	if transferContracts.IsFirstContract() {
		// Create rejection of initial transfer request.
		etx, err := a.createRejection(ctx, transferTransaction, transferOutputIndex,
			transferContracts.CurrentOutputIndex(), rejectError)
		if err != nil {
			return nil, errors.Wrap(err, "create transfer rejection")
		}

		return etx, nil
	}

	// Create rejection of the settlement request to the previous contract.
	rejectError.ReceiverLockingScript = transferContracts.PriorContractLockingScript()
	rejectError.OutputIndex = -1

	etx, err := a.createRejection(ctx, transaction, outputIndex, -1, rejectError)
	if err != nil {
		return nil, errors.Wrap(err, "create settlement request rejection")
	}

	return etx, nil
}
