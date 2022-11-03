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
	tasks []*Task

	// refreshNotify is used notify the Run thread that the tasks have changed so the next task
	// should be refreshed.
	refreshNotify chan interface{}

	lock sync.Mutex
}

type Task struct {
	typ      uint8
	id       bitcoin.Hash32
	start    time.Time
	function threads.InterruptableFunction
}

type Task interface {
	Equal(other Task) bool
	Start() time.Time
	Run(ctx context.Context, interrupt <-chan interface{}) error
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		refreshNotify: make(chan interface{}, 1),
	}
}

func (s *Scheduler) Schedule(ctx context.Context, id bitcoin.Hash32, start time.Time,
	function threads.InterruptableFunction) {

	tsk := &Task{
		typ: typ,
		id:       id,
		start:    start,
		function: function,
	}

	s.lock.Lock()

	for i, task := range s.tasks {
		if task.id.Equal(&id) {
			// Update existing task
			s.tasks[i] = tsk
			s.lock.Unlock()
			s.refreshNotify <- true
			return
		}
	}

	// Add new task
	s.tasks = append(s.tasks, tsk)
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
		if task.id.Equal(&id) {
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
			if now.After(nextTask.start) {
				if err := nextTask.function(ctx, interrupt); err != nil {
					if errors.Cause(err) == threads.Interrupted {
						return threads.Interrupted
					}

					return errors.Wrap(err, nextTask.id.String())
				}

				s.remove(nextTask.id)
			}

			durationToStart := nextTask.start.Sub(now)
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
			if err := nextTask.function(ctx, interrupt); err != nil {
				if errors.Cause(err) == threads.Interrupted {
					return threads.Interrupted
				}

				return errors.Wrap(err, nextTask.id.String())
			}

			s.remove(nextTask.id)
		}
	}
}

func (s *Scheduler) findNextTask() *Task {
	s.lock.Lock()
	defer s.lock.Unlock()

	var result *Task
	for _, task := range s.tasks {
		if result == nil || result.start.After(task.start) {
			result = task
		}
	}

	return result
}
