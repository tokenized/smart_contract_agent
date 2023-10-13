package agents

import (
	"context"
	"sync"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/smart_contract_agent/internal/platform"
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

	store                 Store
	contractLockingScript bitcoin.Script
	transferTxID          bitcoin.Hash32

	lock sync.Mutex
}

func NewCancelPendingTransferTask(start time.Time, store Store,
	contractLockingScript bitcoin.Script, transferTxID bitcoin.Hash32) *CancelPendingTransferTask {

	return &CancelPendingTransferTask{
		start:                 start,
		store:                 store,
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

	store := t.store
	contractLockingScript := t.contractLockingScript
	transferTxID := t.transferTxID

	return CancelPendingTransfer(ctx, store, contractLockingScript, transferTxID)
}

func CancelPendingTransfer(ctx context.Context, store Store, contractLockingScript bitcoin.Script,
	transferTxID bitcoin.Hash32) (*expanded_tx.ExpandedTx, error) {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("trace", uuid.New()))

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("transfer_txid", transferTxID),
		logger.Stringer("contract_locking_script", contractLockingScript),
	}, "Cancelling pending transfer")

	agent, err := store.GetAgent(ctx, contractLockingScript)
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

	ctx = logger.ContextWithLogFields(ctx)

	agentLockingScript := a.LockingScript()

	// Get transfer transaction and action.
	transferTransaction, err := a.transactions.GetTxWithAncestors(ctx, transferTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get tx")
	}

	if transferTransaction == nil {
		return nil, errors.New("Transaction not found")
	}
	defer a.transactions.Release(ctx, transferTxID)

	var transfer *actions.Transfer
	var transferOutputIndex int
	isTest := a.Config().IsTest
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
			transferOutputIndex = i
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

	processeds := transferTransaction.GetContractProcessed(a.ContractHash(), transferOutputIndex)
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
			return nil, errors.New("Response transaction not found")
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
		}, "Action already processed. Not canceling")

		return nil, nil
	}

	if err := a.cancelTransfer(ctx, transferTransaction, transfer); err != nil {
		return nil, errors.Wrap(err, "cancel transfer")
	}

	// Send rejection
	rejectError := platform.NewRejectError(actions.RejectionsTransferExpired, "")
	contractOutputIndex := transferContracts.CurrentOutputIndex()
	etx, err := a.createRejection(ctx, transferTransaction, transferOutputIndex,
		contractOutputIndex, rejectError)
	if err != nil {
		return nil, errors.Wrap(err, "reject")
	}
	logger.WarnWithFields(ctx, []logger.Field{
		logger.Stringer("request_txid", transferTxID),
		logger.Stringer("response_txid", etx.TxID()),
	}, "Sending transfer expired rejection")

	return etx, nil
}
