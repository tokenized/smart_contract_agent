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
	signatureRequest *messages.SignatureRequest, now uint64) error {

	agentLockingScript := a.LockingScript()

	// Deserialize payload transaction.
	tx := &wire.MsgTx{}
	if err := tx.Deserialize(bytes.NewReader(signatureRequest.Payload)); err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error(), now)), "reject")
	}

	if len(tx.TxIn) == 0 {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "settlement missing inputs", now)),
			"reject")
	}

	// Parse settlement action.
	isTest := a.IsTest()
	var settlement *actions.Settlement
	for _, txout := range tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, isTest)
		if err != nil {
			continue
		}

		if s, ok := action.(*actions.Settlement); ok {
			if settlement != nil {
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsMsgMalformed, "more than on settlement",
						now)), "reject")
			}
			settlement = s
		} else {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("non settlement action: %s", action.Code()), now)), "reject")
		}
	}

	if settlement == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "missing settlement action", now)),
			"reject")
	}

	// Add spent outputs to transaction.
	transferTxID := tx.TxIn[0].PreviousOutPoint.Hash
	for _, txin := range tx.TxIn[1:] {
		if !transferTxID.Equal(&txin.PreviousOutPoint.Hash) {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					"settlement inputs not all from transfer tx", now)), "reject")
		}
	}

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("transfer_txid", transferTxID))
	logger.Info(ctx, "Processing signature request")

	transferTransaction, err := a.transactions.Get(ctx, transferTxID)
	if err != nil {
		return errors.Wrap(err, "get transfer tx")
	}

	if transferTransaction == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "unknown transfer tx", now)), "reject")
	}
	defer a.transactions.Release(ctx, transferTxID)

	transferTransaction.Lock()
	transferTx := transferTransaction.GetMsgTx()
	var transfer *actions.Transfer
	for _, txout := range transferTx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, isTest)
		if err != nil {
			continue
		}

		if t, ok := action.(*actions.Transfer); ok {
			transfer = t
			break
		}
	}

	if transfer == nil {
		transferTransaction.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "missing transfer action", now)),
			"reject")
	}
	transferTransaction.Unlock()

	transferContracts, err := parseTransferContracts(transferTransaction, transfer,
		agentLockingScript, now)
	if err != nil {
		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			return errors.Wrap(a.sendRejection(ctx, transaction, rejectError), "reject")
		}

		return errors.Wrap(err, "parse contracts")
	}

	transaction.Lock()
	firstInputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrap(err, "get first input output")
	}

	if !firstInputOutput.LockingScript.Equal(transferContracts.NextLockingScript) {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				"signature request not from next contract", now)), "reject")
	}

	settlementTx, err := txbuilder.NewTxBuilderFromWire(a.FeeRate(), a.DustFeeRate(), tx,
		[]*wire.MsgTx{transferTx})
	if err != nil {
		return errors.Wrap(err, "build settlement tx")
	}

	// Verify the tx has correct settlements for this contract.
	var balances state.Balances
	for index, instrumentSettlement := range settlement.Instruments {
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
				return errors.Wrap(a.sendRejection(instrumentCtx, transaction, rejectError),
					"reject")
			}

			return errors.Wrapf(err, "verify settlement: %s", instrumentID)
		}

		defer a.balances.ReleaseMulti(instrumentCtx, agentLockingScript, instrumentCode,
			instrumentBalances)
		balances = state.AppendBalances(balances, instrumentBalances)
	}

	// Sign settlement tx.
	if _, err := settlementTx.SignOnly([]bitcoin.Key{a.Key()}); err != nil {
		balances.Unlock()

		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error(), now)),
				"reject")
		}

		return errors.Wrap(err, "sign")
	}

	// If this is the first contract then ensure settlement tx is complete and broadcast.
	if transferContracts.IsFirstContract() {
		if err := a.completeSettlement(ctx, transferTxID, settlementTx, balances, now); err != nil {
			balances.Unlock()
			return errors.Wrap(err, "complete settlement")
		}

		balances.Unlock()
		return nil
	}

	balances.Unlock()

	// If this isn't the first contract then create a signature request to the previous contract.
	if err := a.sendSignatureRequest(ctx, transaction, transferContracts, settlementTx,
		now); err != nil {
		return errors.Wrap(err, "send signature request")
	}

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
				"invalid settlement index", now)
		}

		txout := settlementTx.MsgTx.TxOut[settlement.Index]
		lockingScripts = appendLockingScript(lockingScripts, txout.LockingScript)
	}

	balances, err := a.balances.GetMulti(ctx, agentLockingScript, instrumentCode, lockingScripts)
	if err != nil {
		return nil, errors.Wrap(err, "get balances")
	}

	balances.Lock()

	for i, settlement := range instrumentSettlement.Settlements {
		txout := settlementTx.MsgTx.TxOut[settlement.Index]

		balance := balances.Find(txout.LockingScript)
		if balance == nil {
			balances.Unlock()
			a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Int("index", i),
				logger.Stringer("locking_script", txout.LockingScript),
			}, "Missing settlement balance")
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("missing balance %d", i), now)
		}

		adjustment := balance.VerifySettlement(transferTxID)
		if adjustment == nil {
			balances.Unlock()
			a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Int("index", i),
				logger.Stringer("locking_script", txout.LockingScript),
			}, "Missing settlement adjustment")
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("missing adjustment %d", i), now)
		}

		if adjustment.SettledQuantity != settlement.Quantity {
			balances.Unlock()
			a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Int("index", i),
				logger.Stringer("locking_script", txout.LockingScript),
				logger.Uint64("adjustment_quantity", adjustment.SettledQuantity),
				logger.Uint64("settlement_quantity", settlement.Quantity),
			}, "Wrong settlement quantity")
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("wrong settlement quantity %d", i), now)
		}
	}

	return balances, nil
}

func (a *Agent) sendSignatureRequest(ctx context.Context, currentTransaction *state.Transaction,
	transferContracts *TransferContracts, settlementTx *txbuilder.TxBuilder, now uint64) error {

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

	if err := messageTx.AddOutput(messageScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add message output")
	}

	if _, err := messageTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, currentTransaction,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error(), now)),
				"reject")
		}

		return errors.Wrap(err, "sign")
	}

	messageTxID := *messageTx.MsgTx.TxHash()
	if _, err := a.transactions.AddRaw(ctx, messageTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, messageTxID)

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("previous_contract_locking_script",
			transferContracts.PreviousLockingScript),
		logger.Stringer("response_txid", messageTxID),
	}, "Sending signature request to previous contract")
	if err := a.BroadcastTx(ctx, messageTx.MsgTx); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	return nil
}
