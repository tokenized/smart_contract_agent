package agents

import (
	"context"
	"sync"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// CancelPendingTransferTask cancels a pending multi-contract transfer when the other contract(s)
// don't respond quickly enough.
//
// When a settlement request is sent, signifying that this contract agent is waiting on other
// contract agents to complete the settlement, then this event is scheduled to cancel the transfer
// if it expires before the other contract agents approve the settlement.
//
// When a signature request is received and completed, signifying that all other contract agents
// have approved the settlement, then this event is cancelled because this contract agent has
// already signed final approval of the settlement and can no longer cancel the transfer because it
// can't control if that tx will be completed and broadcast.
type CancelPendingTransferTask struct {
	start time.Time

	factory               AgentFactory
	contractLockingScript bitcoin.Script
	transferTxID          bitcoin.Hash32

	lock sync.Mutex
}

func NewCancelPendingTransferTask(start time.Time, factory AgentFactory,
	contractLockingScript bitcoin.Script, transferTxID bitcoin.Hash32) *CancelPendingTransferTask {

	return &CancelPendingTransferTask{
		start:                 start,
		factory:               factory,
		contractLockingScript: contractLockingScript,
		transferTxID:          transferTxID,
	}
}

func (t *CancelPendingTransferTask) ID() bitcoin.Hash32 {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.transferTxID
}

func (t *CancelPendingTransferTask) Start() time.Time {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.start
}

func (t *CancelPendingTransferTask) ContractLockingScript() bitcoin.Script {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.contractLockingScript
}

func (t *CancelPendingTransferTask) Run(ctx context.Context,
	interrupt <-chan interface{}) (*expanded_tx.ExpandedTx, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	factory := t.factory
	contractLockingScript := t.contractLockingScript
	transferTxID := t.transferTxID

	return CancelPendingTransfer(ctx, factory, contractLockingScript, transferTxID)
}

func CancelPendingTransfer(ctx context.Context, factory AgentFactory,
	contractLockingScript bitcoin.Script, transferTxID bitcoin.Hash32) (*expanded_tx.ExpandedTx, error) {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("trace", uuid.New()))

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("transfer_txid", transferTxID),
		logger.Stringer("contract_locking_script", contractLockingScript),
	}, "Cancelling pending transfer")

	agent, err := factory.GetAgent(ctx, contractLockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "get agent")
	}

	if agent == nil {
		return nil, errors.New("Agent not found")
	}
	defer agent.Release(ctx)

	return agent.CancelPendingTransfer(ctx, transferTxID)
}

func (a *Agent) CancelPendingTransfer(ctx context.Context,
	transferTxID bitcoin.Hash32) (*expanded_tx.ExpandedTx, error) {

	agentLockingScript := a.LockingScript()
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("transfer_txid", transferTxID),
		logger.Stringer("contract_locking_script", agentLockingScript))

	logger.Info(ctx, "Canceling pending transaction")

	// Get transfer transaction and action.
	transferTransaction, err := a.caches.Transactions.Get(ctx, transferTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get tx")
	}

	if transferTransaction == nil {
		return nil, errors.New("Transaction not found")
	}
	defer a.caches.Transactions.Release(ctx, transferTxID)

	var transfer *actions.Transfer
	isTest := a.IsTest()
	transferTransaction.Lock()
	outputCount := transferTransaction.OutputCount()
	for i := 0; i < outputCount; i++ {
		output := transferTransaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.Transfer); ok {
			transfer = a
		}
	}
	transferTransaction.Unlock()

	if transfer == nil {
		return nil, errors.New("Missing transfer action")
	}

	transferContracts, err := parseTransferContracts(transferTransaction, transfer,
		agentLockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "parse contracts")
	}

	// Collect balances effected by transfer.
	allBalances := make(state.BalanceSet, len(transfer.Instruments))
	for index, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			continue
		}

		if !agentLockingScript.Equal(transferContracts.Outputs[index].LockingScript) {
			continue
		}

		instrumentID, _ := protocol.InstrumentIDForTransfer(instrumentTransfer)
		instrumentCtx := logger.ContextWithLogFields(ctx,
			logger.String("instrument_id", instrumentID))
		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], instrumentTransfer.InstrumentCode)

		var lockingScripts []bitcoin.Script
		for _, sender := range instrumentTransfer.InstrumentSenders {
			inputOutput, err := transferTransaction.InputOutput(int(sender.Index))
			if err != nil {
				logger.Warn(instrumentCtx, "Invalid sender index : %s", err)
				continue
			}

			lockingScripts = appendLockingScript(lockingScripts, inputOutput.LockingScript)
		}

		for _, receiver := range instrumentTransfer.InstrumentReceivers {
			receiverAddress, err := bitcoin.DecodeRawAddress(receiver.Address)
			if err != nil {
				logger.Warn(instrumentCtx, "Invalid receiver address : %s", err)
				continue
			}

			lockingScript, err := receiverAddress.LockingScript()
			if err != nil {
				logger.Warn(instrumentCtx, "Invalid receiver address script : %s", err)
				continue
			}

			lockingScripts = appendLockingScript(lockingScripts, lockingScript)
		}

		balances, err := a.caches.Balances.GetMulti(instrumentCtx, agentLockingScript,
			instrumentCode, lockingScripts)
		if err != nil {
			return nil, errors.Wrap(err, "get balances")
		}
		defer a.caches.Balances.ReleaseMulti(instrumentCtx, agentLockingScript, instrumentCode,
			balances)

		allBalances[index] = balances
	}

	lockerResponseChannel := a.balanceLocker.AddRequest(allBalances)
	lockerResponse := <-lockerResponseChannel
	switch v := lockerResponse.(type) {
	case uint64:
	case error:
		return nil, errors.Wrap(v, "balance locker")
	}

	// Cancel pending transfers associated with this transfer txid.
	allBalances.CancelPending(transferTxID)
	allBalances.Unlock()

	return nil, nil
}
