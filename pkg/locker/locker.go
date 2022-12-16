package locker

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

var (
	ErrShuttingDown        = errors.New("Shutting Down")
	ErrRequestsOutstanding = errors.New("Requests Outstanding")
)

type Lockable interface {
	Lock()
}

// Locker prevents deadlocks when locking balances.
//
// Whenever a group of items are being locked to perform a single action and there are multiple
// threads doing the locking then there is the chance that one thread will lock item A and be
// waiting on the lock of item B while another thread has a lock on the item B and is waiting for a
// lock on the item A. This will never resolve and is a deadlock.
//
// Locker prevents this by doing all of the locking in one thread so it is never possible for
// one thread to be part of the way through a group lock when another thread attempts a group lock.
// Since each agent will only lock its own objects there can be one balance locker thread per agent.
type Locker interface {
	AddRequest(balances Lockable) <-chan interface{}
}

// InlineLocker just locks the balances in the same thread that calls AddRequest. It should
// only be used when there is only one thread processing transactions otherwise it doesn't serve its
// purpose of preventing dead locks.
type InlineLocker struct{}

func NewInlineLocker() *InlineLocker {
	return &InlineLocker{}
}

func (bl *InlineLocker) AddRequest(lockable Lockable) <-chan interface{} {
	response := make(chan interface{}, 1)
	lockable.Lock()
	response <- uint64(time.Now().UnixNano()) // return new now
	return response
}

// ThreadedLocker runs a thread that waits for group lock requests to be written to a channel
// and locks them as they are received, then notifies the requestor that the lock is complete. The
// requestor is then responsible for releasing the lock when the operation is complete. Any
// conflicting group locks will wait for the previous lock to be released, but that wait should be
// minimal as all of the data fetching and most of the validation should happen before the lock.
type ThreadedLocker struct {
	requests chan *lockRequest
}

type lockRequest struct {
	Lockable Lockable
	Response chan<- interface{}
}

func NewThreadedLocker(size int) *ThreadedLocker {
	return &ThreadedLocker{
		requests: make(chan *lockRequest, size),
	}
}

func (bl *ThreadedLocker) AddRequest(lockable Lockable) <-chan interface{} {
	response := make(chan interface{}, 1)
	bl.requests <- &lockRequest{
		Lockable: lockable,
		Response: response,
	}
	return response
}

func (bl *ThreadedLocker) Run(ctx context.Context, interrupt <-chan interface{}) error {
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
			request.Execute(ctx)
		}
	}
}

func (r *lockRequest) Execute(ctx context.Context) {
	// TODO Research using TryLock (available in golang 1.18) to check if there is a lock conflict
	// and then unlocking some other requests first. These locks should only have to wait for the
	// finalization of the action, and not the data fetching or most of the validation, so there is
	// limited benefit to making it more complex. --ce
	r.Lockable.Lock()
	r.Response <- uint64(time.Now().UnixNano()) // return new now
}
