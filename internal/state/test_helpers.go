package state

import (
	"context"
	"fmt"
	"time"

	"github.com/tokenized/pkg/storage"
)

// StartTestCaches starts all the caches and wraps them into one interrupt and complete.
func StartTestCaches(ctx context.Context, store storage.StreamStorage, requestThreadCount int,
	requestTimeout time.Duration, expireCount int,
	expiration time.Duration) (*ContractCache, *BalanceCache, *TransactionCache, *SubscriptionCache,
	chan<- interface{}, <-chan interface{}) {

	contracts, err := NewContractCache(store, 4, 2*time.Second, 10000, 10*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to create contract cache : %s", err))
	}

	balances, err := NewBalanceCache(store, 4, 2*time.Second, 10000, 10*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to create balance cache : %s", err))
	}

	transactions, err := NewTransactionCache(store, 4, 2*time.Second, 10000, 10*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to create transaction cache : %s", err))
	}

	subscriptions, err := NewSubscriptionCache(store, 4, 2*time.Second, 10000, 10*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to create subscription cache : %s", err))
	}

	contractsInterrupt := make(chan interface{})
	contractsComplete := make(chan interface{})
	go func() {
		if err := contracts.Run(ctx, contractsInterrupt); err != nil {
			panic(fmt.Sprintf("Contracts cache returned an error : %s", err))
		}
		close(contractsComplete)
	}()

	balancesInterrupt := make(chan interface{})
	balancesComplete := make(chan interface{})
	go func() {
		if err := balances.Run(ctx, balancesInterrupt); err != nil {
			panic(fmt.Sprintf("Balances cache returned an error : %s", err))
		}
		close(balancesComplete)
	}()

	transactionsInterrupt := make(chan interface{})
	transactionsComplete := make(chan interface{})
	go func() {
		if err := transactions.Run(ctx, transactionsInterrupt); err != nil {
			panic(fmt.Sprintf("Transactions cache returned an error : %s", err))
		}
		close(transactionsComplete)
	}()

	subscriptionsInterrupt := make(chan interface{})
	subscriptionsComplete := make(chan interface{})
	go func() {
		if err := subscriptions.Run(ctx, subscriptionsInterrupt); err != nil {
			panic(fmt.Sprintf("Subscriptions cache returned an error : %s", err))
		}
		close(subscriptionsComplete)
	}()

	interrupt := make(chan interface{})
	complete := make(chan interface{})
	go func() {
		select {
		case <-contractsComplete:
			panic("Contracts cache completed early")
		case <-balancesComplete:
			panic("Balances cache completed early")
		case <-transactionsComplete:
			panic("Transactions cache completed early")
		case <-subscriptionsComplete:
			panic("Subscriptions cache completed early")
		case <-interrupt:
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
