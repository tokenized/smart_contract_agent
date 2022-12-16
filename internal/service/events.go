package service

import (
	"context"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	"github.com/tokenized/smart_contract_agent/pkg/scheduler"

	"github.com/pkg/errors"
)

func (s *Service) LoadEvents(ctx context.Context) error {
	events, err := state.ListEvents(ctx, s.store)
	if err != nil {
		if errors.Cause(err) == storage.ErrNotFound {
			return nil
		}

		return errors.Wrap(err, "list events")
	}

	for _, event := range events {
		switch event.Type {
		case scheduler.EventTypeVoteCutOff:
			if err := loadVoteCutOffTask(ctx, s.scheduler, event.Start, s,
				event.ContractLockingScript, event.ID); err != nil {
				return errors.Wrap(err, "load vote cut off")
			}

		case scheduler.EventTypeTransferExpiration:
			if err := loadCancelPendingTransferTask(ctx, s.scheduler, event.Start, s,
				event.ContractLockingScript, event.ID); err != nil {
				return errors.Wrap(err, "load cancel pending transfer")
			}

		}
	}

	return nil
}

func (s *Service) SaveEvents(ctx context.Context) error {
	tasks := s.scheduler.ListTasks()

	var events scheduler.Events
	for _, task := range tasks {
		event := &scheduler.Event{
			Start: uint64(task.Start().UnixNano()),
			ID:    task.ID(),
		}

		switch t := task.(type) {
		case *agents.FinalizeVoteTask:
			event.Type = scheduler.EventTypeVoteCutOff
			event.ContractLockingScript = t.ContractLockingScript()
		case *agents.CancelPendingTransferTask:
			event.Type = scheduler.EventTypeTransferExpiration
			event.ContractLockingScript = t.ContractLockingScript()
		default:
			logger.Error(ctx, "Unsupported event type")
			continue
		}

		events = append(events, event)
	}

	if err := state.SaveEvents(ctx, s.store, events); err != nil {
		return errors.Wrap(err, "save")
	}

	return nil
}

func loadVoteCutOffTask(ctx context.Context, scheduler *scheduler.Scheduler, startTimestamp uint64,
	store agents.Store, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32) error {

	cutOffTime := time.Unix(0, int64(startTimestamp))
	task := agents.NewFinalizeVoteTask(cutOffTime, store, contractLockingScript,
		voteTxID)

	scheduler.Schedule(ctx, task)
	return nil
}

func loadCancelPendingTransferTask(ctx context.Context, scheduler *scheduler.Scheduler,
	startTimestamp uint64, store agents.Store, contractLockingScript bitcoin.Script,
	transferTxID bitcoin.Hash32) error {

	expirationTime := time.Unix(0, int64(startTimestamp))
	task := agents.NewCancelPendingTransferTask(expirationTime, store, contractLockingScript,
		transferTxID)

	scheduler.Schedule(ctx, task)
	return nil
}
