package agents

import (
	"bytes"
	"context"
	"fmt"
	"time"

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

func (a *Agent) processSettlementRequest(ctx context.Context, transaction *state.Transaction,
	outputIndex int, settlementRequest *messages.SettlementRequest, now uint64) error {

	agentLockingScript := a.LockingScript()
	ra, err := bitcoin.RawAddressFromLockingScript(agentLockingScript)
	if err != nil {
		return errors.Wrap(err, "agent raw address")
	}

	settlementTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	newTransferTxID, err := bitcoin.NewHash32(settlementRequest.TransferTxId)
	if err != nil {
		logger.Warn(ctx, "Invalid transfer txid in settlement request : %s", err)
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "transfer txid invalid"), now),
			"reject")
	}
	transferTxID := *newTransferTxID

	settlementAction, err := protocol.Deserialize(settlementRequest.Settlement, a.IsTest())
	if err != nil {
		logger.Warn(ctx, "Failed to decode settlement from settlement request : %s", err)
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "settlement invalid"), now),
			"reject")
	}

	settlement, ok := settlementAction.(*actions.Settlement)
	if !ok {
		logger.Warn(ctx, "Settlement request settlement is not a settlement : %s", err)
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "settlement wrong"), now),
			"reject")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("transfer_txid", transferTxID),
	}, "TransferTxID")

	transferTransaction, err := a.caches.Transactions.Get(ctx, transferTxID)
	if err != nil {
		return errors.Wrap(err, "get transfer tx")
	}

	if transferTransaction == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("transfer_txid", transferTxID),
		}, "Transfer tx not found")
	}
	defer a.caches.Transactions.Release(ctx, transferTxID)

	isTest := a.IsTest()
	var transfer *actions.Transfer
	transferTransaction.Lock()
	outputCount := transferTransaction.OutputCount()
	for i := 0; i < outputCount; i++ {
		output := transferTransaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			continue
		}

		tfr, ok := action.(*actions.Transfer)
		if ok {
			transfer = tfr
			break
		}
	}
	transferTransaction.Unlock()

	if transfer == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("transfer_txid", transferTxID),
		}, "Transfer action not found in transfer transaction")
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "transfer tx missing transfer"),
			now), "reject")
	}

	transferContracts, err := parseTransferContracts(transferTransaction, transfer,
		agentLockingScript, now)
	if err != nil {
		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex, rejectError, now),
				"reject")
		}

		return errors.Wrap(err, "parse contracts")
	}

	transaction.Lock()
	firstInputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrap(err, "get first input output")
	}

	if !firstInputOutput.LockingScript.Equal(transferContracts.PreviousLockingScript) {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				"settlement request not from previous contract"), now), "reject")
	}

	if err := a.CheckContractIsAvailable(now); err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex, err, now), "reject")
	}

	isFinalContract := transferContracts.IsFinalContract()

	headers := platform.NewHeadersCache(a.headers)

	var balances state.Balances
	for _, contractLockingScript := range transferContracts.LockingScripts {
		for instrumentIndex, contractOutput := range transferContracts.Outputs {
			if !contractOutput.LockingScript.Equal(contractLockingScript) {
				continue
			}

			instrumentTransfer := transfer.Instruments[instrumentIndex]
			var instrumentCode state.InstrumentCode
			copy(instrumentCode[:], instrumentTransfer.InstrumentCode)
			instrumentID, _ := protocol.InstrumentIDForTransfer(instrumentTransfer)
			instrumentCtx := logger.ContextWithLogFields(ctx,
				logger.String("instrument_id", instrumentID))

			instrumentSettlement := getInstrumentSettlement(settlement,
				instrumentTransfer.InstrumentType, instrumentTransfer.InstrumentCode)

			if agentLockingScript.Equal(contractLockingScript) {
				if instrumentSettlement != nil {
					balances.RevertPending(transferTxID)
					balances.Unlock()
					logger.Warn(instrumentCtx, "Settlement already exists in settlment request")
					return errors.Wrap(a.sendRejection(instrumentCtx, transaction, outputIndex,
						platform.NewRejectError(actions.RejectionsMsgMalformed,
							"settlement provided by other contract agent"), now), "reject")
				}

				instrumentSettlement, instrumentBalances, err := a.buildInstrumentSettlement(instrumentCtx,
					settlementTx, settlement, transferTransaction, instrumentCode,
					instrumentTransfer, transferContracts.Outputs[instrumentIndex], true, headers,
					now)
				if err != nil {
					balances.RevertPending(transferTxID)
					balances.Unlock()
					if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
						return errors.Wrap(a.sendRejection(instrumentCtx, transaction, outputIndex,
							rejectError, now), "reject")
					}
					return errors.Wrapf(err, "build settlement: %s", instrumentID)
				}

				settlement.Instruments = append(settlement.Instruments, instrumentSettlement)
				defer a.caches.Balances.ReleaseMulti(instrumentCtx, agentLockingScript,
					instrumentCode, instrumentBalances)
				balances = state.AppendBalances(balances, instrumentBalances)

			} else {
				if instrumentSettlement == nil {
					balances.RevertPending(transferTxID)
					balances.Unlock()
					logger.Warn(instrumentCtx,
						"Settlement for prior external contract doesn't exist in settlment request")
					return errors.Wrap(a.sendRejection(instrumentCtx, transaction, outputIndex,
						platform.NewRejectError(actions.RejectionsMsgMalformed,
							"settlement not provided by other contract"), now), "reject")
				}

				if err := a.buildExternalSettlement(instrumentCtx, settlementTx,
					transferTransaction, instrumentTransfer,
					transferContracts.Outputs[instrumentIndex]); err != nil {
					balances.RevertPending(transferTxID)
					balances.Unlock()
					return errors.Wrapf(err, "build external settlement: %s", instrumentID)
				}
			}
		}

		if agentLockingScript.Equal(contractLockingScript) {
			break
		}
	}

	if isFinalContract {
		// Add settlement
		settlementScript, err := protocol.Serialize(settlement, a.IsTest())
		if err != nil {
			balances.RevertPending(transferTxID)
			balances.Unlock()
			return errors.Wrap(err, "serialize settlement")
		}

		if err := settlementTx.AddOutput(settlementScript, 0, false, false); err != nil {
			balances.RevertPending(transferTxID)
			balances.Unlock()
			return errors.Wrap(err, "add settlement output")
		}

		if transfer.ExchangeFee > 0 {
			ra, err := bitcoin.DecodeRawAddress(transfer.ExchangeFeeAddress)
			if err != nil {
				logger.Warn(ctx, "Invalid exchange fee address : %s", err)
				balances.RevertPending(transferTxID)
				balances.Unlock()
				return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
					platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error()), now),
					"reject")
			}

			lockingScript, err := ra.LockingScript()
			if err != nil {
				logger.Warn(ctx, "Invalid exchange fee locking script : %s", err)
				balances.RevertPending(transferTxID)
				balances.Unlock()
				return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
					platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error()), now),
					"reject")
			}

			if err := settlementTx.AddOutput(lockingScript, transfer.ExchangeFee, false,
				false); err != nil {
				balances.RevertPending(transferTxID)
				balances.Unlock()
				return errors.Wrap(err, "add exchange fee")
			}
		}

		for _, contractFee := range settlementRequest.ContractFees {
			if contractFee.Quantity == 0 {
				continue
			}

			ra, err := bitcoin.DecodeRawAddress(contractFee.Address)
			if err != nil {
				logger.Warn(ctx, "Invalid contract fee address : %s", err)
				balances.RevertPending(transferTxID)
				balances.Unlock()
				return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
					platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error()), now),
					"reject")
			}

			lockingScript, err := ra.LockingScript()
			if err != nil {
				logger.Warn(ctx, "Invalid contract fee locking script : %s", err)
				balances.RevertPending(transferTxID)
				balances.Unlock()
				return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
					platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error()), now),
					"reject")
			}

			if err := settlementTx.AddOutput(lockingScript, contractFee.Quantity, false,
				false); err != nil {
				balances.RevertPending(transferTxID)
				balances.Unlock()
				return errors.Wrap(err, "add contract fee")
			}
		}

		// Add the contract fee for this agent.
		if a.ContractFee() > 0 {
			if err := settlementTx.AddOutput(a.FeeLockingScript(), a.ContractFee(), true,
				false); err != nil {
				balances.RevertPending(transferTxID)
				balances.Unlock()
				return errors.Wrap(err, "add contract fee")
			}
		} else if err := settlementTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
			balances.RevertPending(transferTxID)
			balances.Unlock()
			return errors.Wrap(err, "set change")
		}

		// Sign settlement tx.
		key := a.Key()
		usedKeys, err := settlementTx.Sign([]bitcoin.Key{key})
		if err != nil && errors.Cause(err) != txbuilder.ErrMissingPrivateKey {
			balances.RevertPending(transferTxID)
			balances.Unlock()

			if errors.Cause(err) == txbuilder.ErrInsufficientValue {
				logger.Warn(ctx, "Insufficient tx funding : %s", err)
				return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
					platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding,
						err.Error()), now), "reject")
			}

			return errors.Wrap(err, "sign")
		}

		if len(usedKeys) != 1 || !usedKeys[0].Equal(key) {
			balances.RevertPending(transferTxID)
			balances.Unlock()
			return fmt.Errorf("Wrong used key returned from signing : %s", usedKeys[0].PublicKey())
		}

		if err := a.sendSignatureRequest(ctx, transaction, outputIndex, transferContracts,
			settlementTx, now); err != nil {
			balances.RevertPending(transferTxID)
			balances.Unlock()
			return errors.Wrap(err, "send signature request")
		}

		balances.FinalizePending(transferTxID, true)
		balances.Unlock()
		return nil
	}

	// Create settlement request for the next contract agent.
	settlementRequest.ContractFees = append(settlementRequest.ContractFees,
		&messages.TargetAddressField{
			Address:  ra.Bytes(),
			Quantity: a.ContractFee(),
		})

	if err := a.sendSettlementRequest(ctx, transaction, transferTransaction, outputIndex, transfer,
		transferContracts, settlement, now); err != nil {
		balances.RevertPending(transferTxID)
		balances.Unlock()
		return errors.Wrap(err, "send settlement request")
	}

	balances.FinalizePending(transferTxID, true)
	balances.Unlock()
	return nil
}

func getInstrumentSettlement(settlement *actions.Settlement, instrumentType string,
	instrumentCode []byte) *actions.InstrumentSettlementField {
	for _, instrumentSettlement := range settlement.Instruments {
		if instrumentSettlement.InstrumentType == instrumentType &&
			bytes.Equal(instrumentSettlement.InstrumentCode, instrumentCode) {
			return instrumentSettlement
		}
	}

	return nil
}

// buildExternalSettlement updates the settlementTx for an instrument settlement by another contract
// agent.
func (a *Agent) buildExternalSettlement(ctx context.Context, settlementTx *txbuilder.TxBuilder,
	transferTransaction *state.Transaction, instrumentTransfer *actions.InstrumentTransferField,
	contractOutput *wire.TxOut) error {

	transferTxID := transferTransaction.GetTxID()
	if _, err := addResponseInput(settlementTx, transferTxID, contractOutput,
		int(instrumentTransfer.ContractIndex)); err != nil {
		return errors.Wrap(err, "add response input")
	}

	transferTransaction.Lock()
	for i, sender := range instrumentTransfer.InstrumentSenders {
		inputOutput, err := transferTransaction.InputOutput(int(sender.Index))
		if err != nil {
			transferTransaction.Unlock()
			return errors.Wrapf(err, "get sender input output %d", i)
		}

		if _, err := addDustLockingScript(settlementTx, inputOutput.LockingScript); err != nil {
			transferTransaction.Unlock()
			return errors.Wrapf(err, "add sender locking script %d", i)
		}
	}
	transferTransaction.Unlock()

	for i, receiver := range instrumentTransfer.InstrumentReceivers {
		ra, err := bitcoin.DecodeRawAddress(receiver.Address)
		if err != nil {
			return errors.Wrapf(err, "address %d", i)
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			return errors.Wrapf(err, "locking script %d", i)
		}

		if _, err := addDustLockingScript(settlementTx, lockingScript); err != nil {
			transferTransaction.Unlock()
			return errors.Wrapf(err, "add receiver locking script %d", i)
		}
	}

	return nil
}

func (a *Agent) sendSettlementRequest(ctx context.Context,
	currentTransaction, transferTransaction *state.Transaction, currentOutputIndex int,
	transfer *actions.Transfer, transferContracts *TransferContracts,
	settlement *actions.Settlement, now uint64) error {

	if len(transferContracts.NextLockingScript) == 0 {
		return errors.New("Next locking script missing for send settlement request")
	}

	agentLockingScript := a.LockingScript()
	currentTxID := currentTransaction.GetTxID()
	transferTxID := transferTransaction.GetTxID()
	var fundingIndex int
	var fundingOutput *wire.TxOut
	if !currentTxID.Equal(&transferTxID) {
		// If the current transaction is a settlement request and not the transfer transaction, then
		// there is no boomerang output, so we just use the only output.
		fundingIndex = 0
		currentTransaction.Lock()
		fundingOutput = currentTransaction.Output(int(fundingIndex))
		currentTransaction.Unlock()
	} else {
		if transferContracts.BoomerangOutput == nil {
			return fmt.Errorf("Multi-Contract Transfer missing boomerang output")
		}

		fundingIndex = transferContracts.BoomerangOutputIndex
		fundingOutput = transferContracts.BoomerangOutput
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("funding_index", fundingIndex),
		logger.Uint64("funding_value", fundingOutput.Value),
	}, "Funding settlement request with output")

	if !fundingOutput.LockingScript.Equal(agentLockingScript) {
		return fmt.Errorf("Wrong locking script for funding output")
	}

	messageTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := messageTx.AddInput(wire.OutPoint{Hash: currentTxID, Index: uint32(fundingIndex)},
		agentLockingScript, fundingOutput.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	if err := messageTx.AddOutput(transferContracts.NextLockingScript, 0, true, true); err != nil {
		return errors.Wrap(err, "add next contract output")
	}

	isTest := a.IsTest()
	settlementScript, err := protocol.Serialize(settlement, isTest)
	if err != nil {
		return errors.Wrap(err, "serialize settlement")
	}

	settlementRequest := &messages.SettlementRequest{
		Timestamp:    now,
		TransferTxId: transferTransaction.GetTxID().Bytes(),
		Settlement:   settlementScript,
	}

	contractFee := a.ContractFee()
	if contractFee != 0 {
		ra, err := bitcoin.RawAddressFromLockingScript(a.FeeLockingScript())
		if err != nil {
			return errors.Wrap(err, "agent raw address")
		}

		settlementRequest.ContractFees = []*messages.TargetAddressField{
			{
				Address:  ra.Bytes(),
				Quantity: contractFee,
			},
		}
	}

	payloadBuffer := &bytes.Buffer{}
	if err := settlementRequest.Serialize(payloadBuffer); err != nil {
		return errors.Wrap(err, "serialize settlement request")
	}

	message := &actions.Message{
		ReceiverIndexes: []uint32{0}, // First output is receiver of message
		MessageCode:     settlementRequest.Code(),
		MessagePayload:  payloadBuffer.Bytes(),
	}

	messageScript, err := protocol.Serialize(message, isTest)
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
	currentTx := currentTransaction.Tx.Copy()
	currentTransaction.Unlock()

	transferTransaction.Lock()
	transferTx := transferTransaction.Tx.Copy()
	transferTransaction.Unlock()

	etx, err := buildExpandedTx(messageTx.MsgTx, []*wire.MsgTx{currentTx, transferTx})
	if err != nil {
		return errors.Wrap(err, "expanded tx")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("next_contract_locking_script", transferContracts.NextLockingScript),
		logger.Stringer("response_txid", messageTxID),
	}, "Sending settlement request to next contract")
	if err := a.BroadcastTx(ctx, etx, message.ReceiverIndexes); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	// Schedule cancel of transfer if other contract(s) don't respond.
	if a.scheduler != nil {
		expireTimeStamp := now + uint64(a.MultiContractExpiration().Nanoseconds())
		expireTime := time.Unix(0, int64(expireTimeStamp))

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("task_start", int64(expireTimeStamp)),
		}, "Scheduling cancel pending transfer")

		task := NewCancelPendingTransferTask(expireTime, a.factory, agentLockingScript,
			transferTxID, expireTimeStamp)
		a.scheduler.Schedule(ctx, task)
	}

	return nil
}
