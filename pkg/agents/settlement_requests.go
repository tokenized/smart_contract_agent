package agents

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/tokenized/bitcoin_interpreter"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/headers"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/messages"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"

	"github.com/pkg/errors"
)

func (a *Agent) processSettlementRequest(ctx context.Context, transaction *transactions.Transaction,
	outputIndex int, settlementRequest *messages.SettlementRequest,
	senderLockingScript, senderUnlockingScript bitcoin.Script) (*expanded_tx.ExpandedTx, error) {

	agentLockingScript := a.LockingScript()
	now := a.Now()

	config := a.Config()
	settlementTx := txbuilder.NewTxBuilder(float32(config.FeeRate), float32(config.DustFeeRate))

	newTransferTxID, err := bitcoin.NewHash32(settlementRequest.TransferTxId)
	if err != nil {
		logger.Warn(ctx, "Invalid transfer txid in settlement request : %s", err)
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "transfer txid invalid")
	}
	transferTxID := *newTransferTxID

	settlementAction, err := protocol.Deserialize(settlementRequest.Settlement, config.IsTest)
	if err != nil {
		logger.Warn(ctx, "Failed to decode settlement from settlement request : %s", err)
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "settlement invalid")
	}

	settlement, ok := settlementAction.(*actions.Settlement)
	if !ok {
		logger.Warn(ctx, "Settlement request settlement is not a settlement : %s", err)
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "settlement wrong")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("transfer_txid", transferTxID),
	}, "TransferTxID")

	transferTransaction, err := a.transactions.Get(ctx, transferTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get transfer tx")
	}

	if transferTransaction == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("transfer_txid", transferTxID),
		}, "Transfer tx not found")
	}
	defer a.transactions.Release(ctx, transferTxID)

	var transfer *actions.Transfer
	var transferOutputIndex int
	transferTransaction.Lock()
	outputCount := transferTransaction.OutputCount()
	for i := 0; i < outputCount; i++ {
		output := transferTransaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, config.IsTest)
		if err != nil {
			continue
		}

		tfr, ok := action.(*actions.Transfer)
		if ok {
			transferOutputIndex = i
			transfer = tfr
			break
		}
	}
	transferTransaction.Unlock()

	if transfer == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("transfer_txid", transferTxID),
		}, "Transfer action not found in transfer transaction")
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"transfer tx missing transfer")
	}

	transferContracts, err := parseTransferContracts(transferTransaction, transfer,
		agentLockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "parse contracts")
	}

	if !senderLockingScript.Equal(transferContracts.PreviousLockingScript) {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"settlement request not from previous contract")
	}

	if isSigHashAll, err := senderUnlockingScript.IsSigHashAll(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, err.Error())
	} else if !isSigHashAll {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, "")
	}

	if err := a.CheckContractIsAvailable(now); err != nil {
		return nil, platform.NewDefaultRejectError(err)
	}

	isFinalContract := transferContracts.IsFinalContract()

	headers := headers.NewHeadersCache(a.headers)
	requiresIdentityOracles, err := a.RequiresIdentityOracles(ctx)
	if err != nil {
		return nil, platform.NewDefaultRejectError(err)
	}

	allBalances := make(state.BalanceSet, len(transferContracts.Outputs))
	instruments := make([]*state.Instrument, len(transferContracts.Outputs))
	instrumentCodes := make([]state.InstrumentCode, len(transferContracts.Outputs))
	allSenderLockingScripts := make([][]bitcoin.Script, len(transferContracts.Outputs))
	allReceiverLockingScripts := make([][]bitcoin.Script, len(transferContracts.Outputs))
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
					allBalances.Revert(transferTxID)
					logger.Warn(instrumentCtx, "Settlement already exists in settlment request")
					return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
						"settlement provided by other contract agent")
				}

				instrument, err := a.caches.Instruments.Get(ctx, agentLockingScript, instrumentCode)
				if err != nil {
					allBalances.Revert(transferTxID)
					return nil, errors.Wrap(err, "get instrument")
				}

				if instrument == nil {
					allBalances.Revert(transferTxID)
					return nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentNotFound,
						"", int(instrumentTransfer.ContractIndex))
				}
				defer a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)

				balances, senderLockingScripts, receiverLockingScripts, err := a.initiateInstrumentTransferBalances(instrumentCtx,
					transferTransaction, instrument, instrumentCode, instrumentTransfer, headers)
				if err != nil {
					allBalances.Revert(transferTxID)
					return nil, errors.Wrapf(err, "build settlement: %s", instrumentID)
				}

				instruments[instrumentIndex] = instrument
				instrumentCodes[instrumentIndex] = instrumentCode
				allBalances[instrumentIndex] = balances
				allSenderLockingScripts[instrumentIndex] = senderLockingScripts
				allReceiverLockingScripts[instrumentIndex] = receiverLockingScripts

				defer a.caches.Balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode,
					balances)
			}
		}

		if agentLockingScript.Equal(contractLockingScript) {
			break
		}
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

	settlement.Timestamp = now

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
				instrumentSettlement, err := a.buildInstrumentSettlement(instrumentCtx,
					allBalances[instrumentIndex], allSenderLockingScripts[instrumentIndex],
					allReceiverLockingScripts[instrumentIndex], settlementTx, transferTxID,
					instruments[instrumentIndex], instrumentCodes[instrumentIndex],
					instrumentTransfer, transferContracts.Outputs[instrumentIndex], true,
					requiresIdentityOracles, headers, now)
				if err != nil {
					allBalances.Revert(transferTxID)
					return nil, errors.Wrapf(err, "build settlement: %s", instrumentID)
				}

				settlement.Instruments = append(settlement.Instruments, instrumentSettlement)
			} else {
				if instrumentSettlement == nil {
					allBalances.Revert(transferTxID)
					logger.Warn(instrumentCtx,
						"Settlement for prior external contract doesn't exist in settlment request")
					return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
						"settlement not provided by other contract")
				}

				if err := a.buildExternalSettlement(instrumentCtx, settlementTx,
					transferTransaction, instrumentTransfer,
					transferContracts.Outputs[instrumentIndex]); err != nil {
					allBalances.Revert(transferTxID)
					return nil, errors.Wrapf(err, "build external settlement: %s", instrumentID)
				}
			}
		}

		if agentLockingScript.Equal(contractLockingScript) {
			break
		}
	}

	if isFinalContract {
		if err := settlement.Validate(); err != nil {
			allBalances.Revert(transferTxID)
			etx, rerr := a.createRejection(ctx, transferTransaction, transferOutputIndex,
				transferContracts.CurrentOutputIndex(),
				platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error()))
			if rerr != nil {
				return nil, errors.Wrap(rerr, "reject")
			}

			return etx, nil
		}

		// Add settlement
		settlementScript, err := protocol.Serialize(settlement, config.IsTest)
		if err != nil {
			allBalances.Revert(transferTxID)
			return nil, errors.Wrap(err, "serialize settlement")
		}

		if err := settlementTx.AddOutput(settlementScript, 0, false, false); err != nil {
			allBalances.Revert(transferTxID)
			return nil, errors.Wrap(err, "add settlement output")
		}

		if transfer.ExchangeFee > 0 {
			ra, err := bitcoin.DecodeRawAddress(transfer.ExchangeFeeAddress)
			if err != nil {
				logger.Warn(ctx, "Invalid exchange fee address : %s", err)
				allBalances.Revert(transferTxID)
				return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
			}

			lockingScript, err := ra.LockingScript()
			if err != nil {
				logger.Warn(ctx, "Invalid exchange fee locking script : %s", err)
				allBalances.Revert(transferTxID)
				return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
			}

			if err := settlementTx.AddOutput(lockingScript, transfer.ExchangeFee, false,
				false); err != nil {
				allBalances.Revert(transferTxID)
				return nil, errors.Wrap(err, "add exchange fee")
			}
		}

		for _, contractFee := range settlementRequest.ContractFees {
			if contractFee.Quantity == 0 {
				continue
			}

			ra, err := bitcoin.DecodeRawAddress(contractFee.Address)
			if err != nil {
				logger.Warn(ctx, "Invalid contract fee address : %s", err)
				allBalances.Revert(transferTxID)
				return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
			}

			lockingScript, err := ra.LockingScript()
			if err != nil {
				logger.Warn(ctx, "Invalid contract fee locking script : %s", err)
				allBalances.Revert(transferTxID)
				return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
			}

			if err := settlementTx.AddOutput(lockingScript, contractFee.Quantity, false,
				false); err != nil {
				allBalances.Revert(transferTxID)
				return nil, errors.Wrap(err, "add contract fee")
			}
		}

		// Add the contract fee for this agent.
		if a.ContractFee() > 0 {
			if err := settlementTx.AddOutput(a.FeeLockingScript(), a.ContractFee(), true,
				false); err != nil {
				allBalances.Revert(transferTxID)
				return nil, errors.Wrap(err, "add contract fee")
			}
		} else if err := settlementTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
			allBalances.Revert(transferTxID)
			return nil, errors.Wrap(err, "set change")
		}

		// Sign settlement tx.
		if err := a.Sign(ctx, settlementTx, a.FeeLockingScript()); err != nil {
			if errors.Cause(err) == txbuilder.ErrInsufficientValue {
				allBalances.Revert(transferTxID)
				return nil, platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding,
					err.Error())
			}

			// The "can't unlock" error is expected here because there are inputs from another
			// agent.
			if errors.Cause(err) != bitcoin_interpreter.CantUnlock {
				allBalances.Revert(transferTxID)
				return nil, errors.Wrap(err, "sign")
			}
		}

		etx, err := a.createSignatureRequest(ctx, transaction, outputIndex, transferContracts,
			settlementTx, now)
		if err != nil {
			allBalances.Revert(transferTxID)
			return nil, errors.Wrap(err, "send signature request")
		}

		allBalances.SettlePending(transferTxID, true)
		return etx, nil
	}

	feeLockingScript := a.FeeLockingScript()
	feeRawAddress, _ := bitcoin.RawAddressFromLockingScript(feeLockingScript)

	// Create settlement request for the next contract agent.
	settlementRequest.ContractFees = append(settlementRequest.ContractFees,
		&messages.TargetAddressField{
			Address:  feeRawAddress.Bytes(),
			Quantity: a.ContractFee(),
		})

	etx, err := a.createSettlementRequest(ctx, transaction, transferTransaction, outputIndex,
		transfer, transferContracts, allBalances, settlement, now)
	if err != nil {
		allBalances.Revert(transferTxID)
		return nil, errors.Wrap(err, "send settlement request")
	}

	return etx, nil
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
	transferTransaction *transactions.Transaction, instrumentTransfer *actions.InstrumentTransferField,
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

func (a *Agent) createSettlementRequest(ctx context.Context,
	currentTransaction, transferTransaction *transactions.Transaction, currentOutputIndex int,
	transfer *actions.Transfer, transferContracts *TransferContracts, balances state.BalanceSet,
	settlement *actions.Settlement, now uint64) (*expanded_tx.ExpandedTx, error) {

	if len(transferContracts.NextLockingScript) == 0 {
		return nil, errors.New("Next locking script missing for send settlement request")
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
			return nil, fmt.Errorf("Multi-Contract Transfer missing boomerang output")
		}

		fundingIndex = transferContracts.BoomerangOutputIndex
		fundingOutput = transferContracts.BoomerangOutput
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("funding_index", fundingIndex),
		logger.Uint64("funding_value", fundingOutput.Value),
	}, "Sending settlement request")

	if !fundingOutput.LockingScript.Equal(agentLockingScript) {
		return nil, fmt.Errorf("Wrong locking script for funding output")
	}

	config := a.Config()
	messageTx := txbuilder.NewTxBuilder(float32(config.FeeRate), float32(config.DustFeeRate))

	if err := messageTx.AddInput(wire.OutPoint{Hash: currentTxID, Index: uint32(fundingIndex)},
		agentLockingScript, fundingOutput.Value); err != nil {
		return nil, errors.Wrap(err, "add input")
	}

	if err := messageTx.AddOutput(transferContracts.NextLockingScript, 0, true, true); err != nil {
		return nil, errors.Wrap(err, "add next contract output")
	}

	settlementScript, err := protocol.Serialize(settlement, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize settlement")
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
			return nil, errors.Wrap(err, "agent raw address")
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
		return nil, errors.Wrap(err, "serialize settlement request")
	}

	message := &actions.Message{
		ReceiverIndexes: []uint32{0}, // First output is receiver of message
		MessageCode:     settlementRequest.Code(),
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

	if err := a.Sign(ctx, messageTx, transferContracts.NextLockingScript); err != nil {
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

	transferTransaction.Lock()
	transferTx := transferTransaction.Tx.Copy()
	transferTransaction.Unlock()

	balances.SettlePending(transferTxID, true)

	etx, err := buildExpandedTx(messageTx.MsgTx, []*wire.MsgTx{&currentTx, &transferTx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("next_contract_locking_script", transferContracts.NextLockingScript),
		logger.Stringer("response_txid", messageTxID),
	}, "Sending settlement request to next contract")

	// Schedule cancel of transfer if other contract(s) don't respond.
	if a.scheduler != nil {
		expireTimeStamp := a.Now() + uint64(config.MultiContractExpiration.Nanoseconds())
		expireTime := time.Unix(0, int64(expireTimeStamp))

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("task_start", int64(expireTimeStamp)),
		}, "Scheduling cancel pending transfer")

		task := NewCancelPendingTransferTask(expireTime, a.agentStore, agentLockingScript,
			transferTxID)
		a.scheduler.Schedule(ctx, task)
	}

	return etx, nil
}

func (a *Agent) createSettlementRequestRejection(ctx context.Context,
	transaction *transactions.Transaction, outputIndex int,
	settlementRequest *messages.SettlementRequest,
	rejectError platform.RejectError) (*expanded_tx.ExpandedTx, error) {

	agentLockingScript := a.LockingScript()

	config := a.Config()

	newTransferTxID, err := bitcoin.NewHash32(settlementRequest.TransferTxId)
	if err != nil {
		logger.Warn(ctx, "Invalid transfer txid in settlement request : %s", err)
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "transfer txid invalid")
	}
	transferTxID := *newTransferTxID

	transferTransaction, err := a.transactions.Get(ctx, transferTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get transfer tx")
	}

	if transferTransaction == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("transfer_txid", transferTxID),
		}, "Transfer tx not found")
	}
	defer a.transactions.Release(ctx, transferTxID)

	var transfer *actions.Transfer
	var transferOutputIndex int
	transferTransaction.Lock()
	outputCount := transferTransaction.OutputCount()
	for i := 0; i < outputCount; i++ {
		output := transferTransaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, config.IsTest)
		if err != nil {
			continue
		}

		tfr, ok := action.(*actions.Transfer)
		if ok {
			transferOutputIndex = i
			transfer = tfr
			break
		}
	}
	transferTransaction.Unlock()

	if transfer == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("transfer_txid", transferTxID),
		}, "Transfer action not found in transfer transaction")
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"transfer tx missing transfer")
	}

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

	// TODO This should also spend the contract's output from the transfer transaction. --ce
	etx, err := a.createRejection(ctx, transaction, outputIndex, -1, rejectError)
	if err != nil {
		return nil, errors.Wrap(err, "create settlement request rejection")
	}

	return etx, nil
}
