package scheduler

import (
	"context"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/threads"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var (
	ErrChannelTimeout = errors.New("Channel Timeout")
)

type Scheduler struct {
	tasks []Task

	// addTaskChannel notifies the run thrad that it needs to add a new task.
	addTaskChannel chan Task

	// removeTaskChannel notifies the run thread that it needs to remove a task.
	removeTaskChannel chan bitcoin.Hash32

	// listTaskChannel notifies the run thread that it needs to list all tasks.
	listTaskChannel chan chan []Task

	// broadcaster is used to broadcast results of an executed task.
	broadcaster Broadcaster

	// channelTimeout is the amount of time after attempting to add to a channel.
	channelTimeout atomic.Value
}

type Task interface {
	ID() bitcoin.Hash32
	Start() time.Time
	Run(ctx context.Context, interrupt <-chan interface{}) (*expanded_tx.ExpandedTx, error)
}

type Broadcaster interface {
	BroadcastTx(context.Context, *expanded_tx.ExpandedTx, []uint32) error
}

func NewScheduler(broadcaster Broadcaster, channelTimeout time.Duration) *Scheduler {
	result := &Scheduler{
		addTaskChannel:    make(chan Task, 1),
		removeTaskChannel: make(chan bitcoin.Hash32, 1),
		listTaskChannel:   make(chan chan []Task, 1),
		broadcaster:       broadcaster,
	}

	result.channelTimeout.Store(channelTimeout)

	return result
}

func (s *Scheduler) ChannelTimeout() time.Duration {
	return s.channelTimeout.Load().(time.Duration)
}

func (s *Scheduler) Schedule(ctx context.Context, task Task) error {
	select {
	case s.addTaskChannel <- task:
		return nil
	case <-time.After(s.ChannelTimeout()):
		return errors.Wrap(ErrChannelTimeout, "adding task")
	}
}

func (s *Scheduler) addTask(ctx context.Context, task Task) {
	id := task.ID()
	for i, t := range s.tasks {
		tid := t.ID()
		if id.Equal(&tid) {
			// Update existing task
			s.tasks[i] = task
			return
		}
	}

	// Add new task
	s.tasks = append(s.tasks, task)
}

func (s *Scheduler) Cancel(ctx context.Context, id bitcoin.Hash32) error {
	select {
	case s.removeTaskChannel <- id:
		return nil
	case <-time.After(s.ChannelTimeout()):
		return errors.Wrap(ErrChannelTimeout, "removing task")
	}
}

func (s *Scheduler) removeTask(ctx context.Context, id bitcoin.Hash32) bool {
	for i, task := range s.tasks {
		tid := task.ID()
		if tid.Equal(&id) {
			s.tasks = append(s.tasks[:i], s.tasks[i+1:]...)
			return true
		}
	}

	return false
}

func (s *Scheduler) ListTasks(ctx context.Context) ([]Task, error) {
	responseChannel := make(chan []Task, 1)
	select {
	case s.listTaskChannel <- responseChannel:
	case <-time.After(s.ChannelTimeout()):
		return nil, errors.Wrap(ErrChannelTimeout, "list tasks request")
	}

	select {
	case tasks := <-responseChannel:
		return tasks, nil
	case <-time.After(s.ChannelTimeout()):
		return nil, errors.Wrap(ErrChannelTimeout, "list tasks response")
	}
}

func (s *Scheduler) listTasks(ctx context.Context) []Task {
	result := make([]Task, len(s.tasks))
	for i, task := range s.tasks {
		result[i] = task
	}

	return result
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
				Chan: reflect.ValueOf(s.addTaskChannel),
			},
			{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(s.removeTaskChannel),
			},
			{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(s.listTaskChannel),
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

				s.removeTask(ctx, task.ID())
			}

			durationToStart := task.Start().Sub(now)
			timer := time.NewTimer(durationToStart)
			selects = append(selects, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(timer.C),
			})
		}

		selectIndex, selectValue, selectValueReceived := reflect.Select(selects)
		switch selectIndex {
		case 0: // interrupt
			return nil

		case 1: // addTaskChannel
			if !selectValueReceived {
				return errors.New("No value received on add task channel")
			}

			selectInterface := selectValue.Interface()
			if selectInterface == nil {
				return errors.New("No value received on add task channel")
			}

			newTask, ok := selectInterface.(Task)
			if !ok {
				return errors.New("Wrong type value received on add task channel")
			}

			s.addTask(ctx, newTask)

		case 2: // removeTaskChannel
			if !selectValueReceived {
				return errors.New("No value received on remove task channel")
			}

			selectInterface := selectValue.Interface()
			if selectInterface == nil {
				return errors.New("No value received on remove task channel")
			}

			taskID, ok := selectInterface.(bitcoin.Hash32)
			if !ok {
				return errors.New("Wrong type value received on remove task channel")
			}

			s.removeTask(ctx, taskID)

		case 3: // listTaskChannel
			if !selectValueReceived {
				return errors.New("No value received on list task channel")
			}

			selectInterface := selectValue.Interface()
			if selectInterface == nil {
				return errors.New("No value received on list task channel")
			}

			responseChannel, ok := selectInterface.(chan []Task)
			if !ok {
				return errors.New("Wrong type value received on list task channel")
			}

			responseChannel <- s.listTasks(ctx)

		case 4: // next task start
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("id", task.ID()),
				logger.Timestamp("start", task.Start().UnixNano()),
			}, "Running scheduled task")

			tctx := logger.ContextWithLogFields(ctx, logger.Stringer("scheduler_trace", uuid.New()))
			etx, runErr := task.Run(tctx, interrupt)
			if etx != nil {
				if berr := s.broadcaster.BroadcastTx(tctx, etx, nil); berr != nil {
					logger.Error(tctx, "Failed to broadcast tx : %s", berr)
				}
			}

			if runErr != nil {
				if errors.Cause(runErr) == threads.Interrupted {
					return threads.Interrupted
				}

				return errors.Wrap(runErr, task.ID().String())
			}

			s.removeTask(ctx, task.ID())
		}
	}
}

func (s *Scheduler) findNextTask() Task {
	var result Task
	for _, task := range s.tasks {
		if result == nil || result.Start().After(task.Start()) {
			result = task
		}
	}

	return result
}
