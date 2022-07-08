package state

import (
	"context"
	"fmt"
	"time"

	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/cacher"
)

// StartTestCaches starts all the caches and wraps them into one interrupt and complete.
func StartTestCaches(ctx context.Context, store storage.StreamStorage, requestThreadCount int,
	requestTimeout time.Duration, expireCount int,
	expiration time.Duration) (*ContractCache, *BalanceCache, *TransactionCache, *SubscriptionCache,
	chan<- interface{}, <-chan interface{}) {

	cacher := cacher.NewCache(store, requestThreadCount, requestTimeout, expireCount, expiration)

	contracts, err := NewContractCache(cacher)
	if err != nil {
		panic(fmt.Sprintf("Failed to create contract cache : %s", err))
	}

	balances, err := NewBalanceCache(cacher)
	if err != nil {
		panic(fmt.Sprintf("Failed to create balance cache : %s", err))
	}

	transactions, err := NewTransactionCache(cacher)
	if err != nil {
		panic(fmt.Sprintf("Failed to create transaction cache : %s", err))
	}

	subscriptions, err := NewSubscriptionCache(cacher)
	if err != nil {
		panic(fmt.Sprintf("Failed to create subscription cache : %s", err))
	}

	interrupt := make(chan interface{})
	complete := make(chan interface{})
	go func() {
		if err := cacher.Run(ctx, interrupt); err != nil {
			panic(fmt.Sprintf("Cacher returned an error : %s", err))
		}
		close(complete)
	}()

	return contracts, balances, transactions, subscriptions, interrupt, complete
}

func StopTestCaches(timeout time.Duration, interrupt chan<- interface{},
	complete <-chan interface{}) {

	close(interrupt)
	select {
	case <-complete:
	case <-time.After(timeout):
		panic("Cache shutdown timed out")
	}
}
