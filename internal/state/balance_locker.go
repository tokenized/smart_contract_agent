package state

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrShuttingDown        = errors.New("Shutting Down")
	ErrRequestsOutstanding = errors.New("Requests Outstanding")
)

// BalanceLocker prevents deadlocks when locking balances.
//
// Whenever a group of items are being locked to perform a single action and there are multiple
// threads doing the locking then there is the chance that one thread will lock item A and be
// waiting on the lock of item B while another thread has a lock on the item B and is waiting for a
// lock on the item A. This will never resolve and is a deadlock.
//
// BalanceLocker prevents this by doing all of the locking in one thread so it is never possible for
// one thread to be part of the way through a group lock when another thread attempts a group lock.
// Since each agent will only lock its own objects there can be one balance locker thread per agent.
type BalanceLocker interface {
	AddRequest(balances BalanceSet) <-chan interface{}
}

// InlineBalanceLocker just locks the balances in the same thread that calls AddRequest. It should
// only be used when there is only one thread processing transactions otherwise it doesn't serve its
// purpose of preventing dead locks.
type InlineBalanceLocker struct{}

func NewInlineBalanceLocker() *InlineBalanceLocker {
	return &InlineBalanceLocker{}
}

func (bl *InlineBalanceLocker) AddRequest(balances BalanceSet) <-chan interface{} {
	response := make(chan interface{}, 1)
	balances.Lock()
	response <- uint64(time.Now().UnixNano()) // return new now
	return response
}

// ThreadedBalanceLocker runs a thread that waits for group lock requests to be written to a channel
// and locks them as they are received, then notifies the requestor that the lock is complete. The
// requestor is then responsible for releasing the lock when the operation is complete. Any
// conflicting group locks will wait for the previous lock to be released, but that wait should be
// minimal as all of the data fetching and most of the validation should happen before the lock.
type ThreadedBalanceLocker struct {
	requests chan *balanceLockRequest
}

type balanceLockRequest struct {
	Balances BalanceSet
	Response chan<- interface{}
}

func NewThreadedBalanceLocker(size int) *ThreadedBalanceLocker {
	return &ThreadedBalanceLocker{
		requests: make(chan *balanceLockRequest, size),
	}
}

func (bl *ThreadedBalanceLocker) AddRequest(balances BalanceSet) <-chan interface{} {
	response := make(chan interface{}, 1)
	bl.requests <- &balanceLockRequest{
		Balances: balances,
		Response: response,
	}
	return response
}

func (bl *ThreadedBalanceLocker) Run(ctx context.Context, interrupt <-chan interface{}) error {
	for {
		select {
		case <-interrupt:
			// Flush any requests
			hadOpenRequests := false
			for {
				select {
				case request := <-bl.requests:
					hadOpenRequests = true
					request.Response <- ErrShuttingDown

				default: // no requests left
					if hadOpenRequests {
						return ErrRequestsOutstanding
					}

					return nil
				}
			}

		case request := <-bl.requests:
			request.Run(ctx)
		}
	}
}

func (r *balanceLockRequest) Run(ctx context.Context) {
	// TODO Research using TryLock (available in golang 1.18) to check if there is a lock conflict
	// and then unlocking some other requests first. These locks should only have to wait for the
	// finalization of the action, and not the data fetching or most of the validation, so there is
	// limited benefit to making it more complex. --ce
	r.Balances.Lock()
	r.Response <- uint64(time.Now().UnixNano()) // return new now
}
