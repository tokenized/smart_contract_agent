package platform

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

type Scheduler struct {
	tasks []Task

	// refreshNotify is used notify the Run thread that the tasks have changed so the next task
	// should be refreshed.
	refreshNotify chan interface{}

	lock sync.Mutex
}

type Task interface {
	ID() bitcoin.Hash32
	Start() time.Time
	Run(ctx context.Context, interrupt <-chan interface{}) error
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		refreshNotify: make(chan interface{}, 1),
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

		nextTask := s.findNextTask()
		if nextTask != nil {
			if now.After(nextTask.Start()) {
				if err := nextTask.Run(ctx, interrupt); err != nil {
					if errors.Cause(err) == threads.Interrupted {
						return threads.Interrupted
					}

					return errors.Wrap(err, nextTask.ID().String())
				}

				s.remove(nextTask.ID())
			}

			durationToStart := nextTask.Start().Sub(now)
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
			if err := nextTask.Run(ctx, interrupt); err != nil {
				if errors.Cause(err) == threads.Interrupted {
					return threads.Interrupted
				}

				return errors.Wrap(err, nextTask.ID().String())
			}

			s.remove(nextTask.ID())
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
