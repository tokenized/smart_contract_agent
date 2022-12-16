package scheduler

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

type Scheduler struct {
	tasks []Task

	// refreshNotify is used notify the Run thread that the tasks have changed so the next task
	// should be refreshed.
	refreshNotify chan interface{}

	broadcaster Broadcaster

	lock sync.Mutex
}

type Task interface {
	ID() bitcoin.Hash32
	Start() time.Time
	Run(ctx context.Context, interrupt <-chan interface{}) (*expanded_tx.ExpandedTx, error)
}

type Broadcaster interface {
	BroadcastTx(context.Context, *expanded_tx.ExpandedTx, []uint32) error
}

func NewScheduler(broadcaster Broadcaster) *Scheduler {
	return &Scheduler{
		refreshNotify: make(chan interface{}, 1),
		broadcaster:   broadcaster,
	}
}

func (s *Scheduler) Schedule(ctx context.Context, task Task) {
	s.lock.Lock()

	id := task.ID()
	for i, t := range s.tasks {
		tid := t.ID()
		if id.Equal(&tid) {
			// Update existing task
			s.tasks[i] = task
			s.lock.Unlock()
			s.refreshNotify <- true
			return
		}
	}

	// Add new task
	s.tasks = append(s.tasks, task)
	s.lock.Unlock()
	s.refreshNotify <- true
}

func (s *Scheduler) Cancel(ctx context.Context, id bitcoin.Hash32) {
	if s.remove(id) {
		s.refreshNotify <- true
	}
}

func (s *Scheduler) remove(id bitcoin.Hash32) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	for i, task := range s.tasks {
		tid := task.ID()
		if tid.Equal(&id) {
			s.tasks = append(s.tasks[:i], s.tasks[i+1:]...)
			return true
		}
	}

	return false
}

func (s *Scheduler) Run(ctx context.Context, interrupt <-chan interface{}) error {
	for {
		now := time.Now()

		selects := []reflect.SelectCase{
			{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(interrupt),
			},
			{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(s.refreshNotify),
			},
		}

		task := s.findNextTask()
		if task != nil {
			if now.After(task.Start()) {
				etx, runErr := task.Run(ctx, interrupt)
				if etx != nil {
					if berr := s.broadcaster.BroadcastTx(ctx, etx, nil); berr != nil {
						logger.Error(ctx, "Failed to broadcast tx : %s", berr)
					}
				}

				if runErr != nil {
					if errors.Cause(runErr) == threads.Interrupted {
						return threads.Interrupted
					}

					return errors.Wrap(runErr, task.ID().String())
				}

				s.remove(task.ID())
			}

			durationToStart := task.Start().Sub(now)
			timer := time.NewTimer(durationToStart)
			selects = append(selects, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(timer.C),
			})
		}

		selectIndex, _, _ := reflect.Select(selects)
		switch selectIndex {
		case 0: // interrupt
			return nil

		case 1: // new task notify
			// continue to reset next task

		case 2: // next task start
			etx, runErr := task.Run(ctx, interrupt)
			if etx != nil {
				if berr := s.broadcaster.BroadcastTx(ctx, etx, nil); berr != nil {
					logger.Error(ctx, "Failed to broadcast tx : %s", berr)
				}
			}

			if runErr != nil {
				if errors.Cause(runErr) == threads.Interrupted {
					return threads.Interrupted
				}

				return errors.Wrap(runErr, task.ID().String())
			}

			s.remove(task.ID())
		}
	}
}

func (s *Scheduler) ListTasks() []Task {
	s.lock.Lock()
	defer s.lock.Unlock()

	result := make([]Task, len(s.tasks))
	for i, task := range s.tasks {
		result[i] = task
	}

	return result
}

func (s *Scheduler) findNextTask() Task {
	s.lock.Lock()
	defer s.lock.Unlock()

	var result Task
	for _, task := range s.tasks {
		if result == nil || result.Start().After(task.Start()) {
			result = task
		}
	}

	return result
}
