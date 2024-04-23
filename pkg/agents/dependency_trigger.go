package agents

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

var (
	ErrChannelTimeout = errors.New("Channel Timeout")
)

type TriggerDependency func(context.Context, transactions.Action) error

type ProcessTransaction func(context.Context, *transactions.Transaction) error

type DependencyTrigger struct {
	threadCount        int
	transactionCache   *transactions.TransactionCache
	completedActions   chan transactions.Action
	channelTimeout     atomic.Value
	processTransaction ProcessTransaction
}

func NewDependencyTrigger(threadCount int, channelTimeout time.Duration,
	transactionCache *transactions.TransactionCache) *DependencyTrigger {

	result := &DependencyTrigger{
		threadCount:      threadCount,
		transactionCache: transactionCache,
		completedActions: make(chan transactions.Action, 100),
	}

	result.channelTimeout.Store(channelTimeout)
	return result
}

func (t *DependencyTrigger) SetTransactionProcessor(processTransaction ProcessTransaction) {
	t.processTransaction = processTransaction
}

func (t *DependencyTrigger) Run(ctx context.Context, interrupt <-chan interface{}) error {
	var selects []reflect.SelectCase
	var wait sync.WaitGroup

	// Start multiple threads running processCompletedTxIDs
	threadList := make([]*threads.InterruptableThread, t.threadCount)
	for i := 0; i < t.threadCount; i++ {
		index := i
		thread, threadComplete := threads.NewInterruptableThreadComplete(fmt.Sprintf("Process %d", index),
			func(ctx context.Context, interrupt <-chan interface{}) error {
				return t.processCompletedActions(ctx, t.completedActions, interrupt)
			}, &wait)
		threadList[i] = thread

		selects = append(selects, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(threadComplete),
		})
	}

	interruptIndex := len(selects)
	selects = append(selects, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(interrupt),
	})

	for _, thread := range threadList {
		thread.Start(ctx)
	}

	selectIndex, selectValue, valueReceived := reflect.Select(selects)
	var selectErr error
	if valueReceived {
		selectInterface := selectValue.Interface()
		if selectInterface != nil {
			err, ok := selectInterface.(error)
			if ok {
				selectErr = err
			}
		}
	}

	if selectIndex < interruptIndex {
		logger.Error(ctx, "DependencyTrigger thread %d Completed : %s", selectIndex, selectErr)
	} else {
		logger.Info(ctx, "Service shutdown requested")
	}

	for _, thread := range threadList {
		thread.Stop(ctx)
	}

	wait.Wait()

	return nil
}

// Trigger triggers any dependent transactions from the transaction id that has been completed.
func (t *DependencyTrigger) Trigger(ctx context.Context, action transactions.Action) error {
	logger.InfoWithFields(ctx, []logger.Field{
		logger.JSON("action", action),
	}, "Triggering dependencies for action")

	channelTimeout := t.channelTimeout.Load().(time.Duration)
	select {
	case t.completedActions <- action.Copy():
		return nil
	case <-time.After(channelTimeout):
		return ErrChannelTimeout
	}
}

func (t *DependencyTrigger) processCompletedActions(ctx context.Context,
	completedActions <-chan transactions.Action, interrupt <-chan interface{}) error {

	for {
		select {
		case action := <-completedActions:
			if err := t.processCompletedAction(ctx, action); err != nil {
				return err
			}
		case <-interrupt:
			return nil
		}
	}
}

func (t *DependencyTrigger) processCompletedAction(ctx context.Context,
	action transactions.Action) error {

	transaction, err := t.transactionCache.Get(ctx, action.TxID)
	if err != nil {
		return errors.Wrapf(err, "get tx: %s", action.TxID)
	}

	if transaction == nil {
		return errors.Wrap(ErrNotFound, action.TxID.String())
	}
	defer t.transactionCache.Release(ctx, action.TxID)

	dependentActions := transaction.GetDependentActions()

	for _, dependentAction := range dependentActions {
		dependentTransaction, err := t.transactionCache.Get(ctx, dependentAction.TxID)
		if err != nil {
			return errors.Wrapf(err, "get dependent tx: %s", dependentAction.TxID)
		}

		if dependentTransaction == nil {
			return errors.Wrapf(ErrNotFound, "dependent: %s", dependentAction.TxID)
		}
		defer t.transactionCache.Release(ctx, dependentAction.TxID)

		if err := t.processTransaction(ctx, dependentTransaction); err != nil {
			return errors.Wrapf(err, "process tx: %s", dependentTransaction.GetTxID())
		}
	}

	return nil
}
