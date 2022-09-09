package state

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tokenized/cacher"
	"github.com/tokenized/pkg/storage"
)

type TestCaches struct {
	Timeout       time.Duration
	Cache         *cacher.Cache
	Contracts     *ContractCache
	Balances      *BalanceCache
	Transactions  *TransactionCache
	Subscriptions *SubscriptionCache
	Interrupt     chan interface{}
	Complete      chan error
	Shutdown      chan error
	StartShutdown chan interface{}
	Wait          sync.WaitGroup

	failed     error
	failedLock sync.Mutex
}

// StartTestCaches starts all the caches and wraps them into one interrupt and complete.
func StartTestCaches(ctx context.Context, t *testing.T, store storage.StreamStorage,
	config cacher.Config, timeout time.Duration) *TestCaches {

	result := &TestCaches{
		Timeout:       timeout,
		Cache:         cacher.NewCache(store, config),
		Interrupt:     make(chan interface{}),
		Complete:      make(chan error, 1),
		Shutdown:      make(chan error, 1),
		StartShutdown: make(chan interface{}),
	}

	var err error
	result.Contracts, err = NewContractCache(result.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create contract cache : %s", err))
	}

	result.Balances, err = NewBalanceCache(result.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create balance cache : %s", err))
	}

	result.Transactions, err = NewTransactionCache(result.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create transaction cache : %s", err))
	}

	result.Subscriptions, err = NewSubscriptionCache(result.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create subscription cache : %s", err))
	}

	go func() {
		defer func() {
			if err := recover(); err != nil {
				t.Errorf("Cache panic : %s", err)
				result.Complete <- fmt.Errorf("panic: %s", err)
			}

			result.Wait.Done()
		}()

		result.Wait.Add(1)
		err := result.Cache.Run(ctx, result.Interrupt, result.Shutdown)
		if err != nil {
			t.Errorf("Cache returned an error : %s", err)
		}
		result.Complete <- err
	}()

	go func() {
		select {
		case err := <-result.Shutdown:
			t.Errorf("Cache shutting down : %s", err)

			if err != nil {
				result.failedLock.Lock()
				result.failed = err
				result.failedLock.Unlock()
			}

		case err, ok := <-result.Complete:
			if ok && err != nil {
				t.Errorf("Cache failed : %s", err)
			} else {
				// StartShutdown should have been triggered first.
				t.Errorf("Cache completed prematurely")
			}

		case <-result.StartShutdown:
			t.Logf("Cache start shutdown triggered")
		}
	}()

	return result
}

func (c *TestCaches) StopTestCaches() {
	close(c.StartShutdown)
	close(c.Interrupt)
	select {
	case err := <-c.Complete:
		if err != nil {
			panic(fmt.Sprintf("Cache failed : %s", err))
		}

	case <-time.After(c.Timeout):
		panic("Cache shutdown timed out")
	}
}

func (c *TestCaches) IsFailed() error {
	c.failedLock.Lock()
	defer c.failedLock.Unlock()

	return c.failed
}
