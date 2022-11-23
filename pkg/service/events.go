package service

import (
	"context"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/agents"

	"github.com/pkg/errors"
)

func (s *Service) LoadEvents(ctx context.Context) error {
	events, err := state.ListEvents(ctx, s.store)
	if err != nil {
		return errors.Wrap(err, "list events")
	}

	for _, event := range events {
		switch event.Type {
		case state.EventTypeVoteCutOff:
			if err := loadVoteCutOffTask(ctx, s.scheduler, event.Start, s,
				event.ContractLockingScript, event.ID); err != nil {
				return errors.Wrap(err, "load vote cut off")
			}

		case state.EventTypeTransferExpiration:
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

	var events state.Events
	for _, task := range tasks {
		event := &state.Event{
			Start: uint64(task.Start().UnixNano()),
			ID:    task.ID(),
		}

		switch t := task.(type) {
		case *agents.FinalizeVoteTask:
			event.Type = state.EventTypeVoteCutOff
			event.ContractLockingScript = t.ContractLockingScript()
		case *agents.CancelPendingTransferTask:
			event.Type = state.EventTypeTransferExpiration
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

func loadVoteCutOffTask(ctx context.Context, scheduler *platform.Scheduler, startTimestamp uint64,
	factory agents.AgentFactory, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32) error {

	cutOffTime := time.Unix(0, int64(startTimestamp))
	task := agents.NewFinalizeVoteTask(cutOffTime, factory, contractLockingScript,
		voteTxID, startTimestamp)

	scheduler.Schedule(ctx, task)
	return nil
}

func loadCancelPendingTransferTask(ctx context.Context, scheduler *platform.Scheduler,
	startTimestamp uint64, factory agents.AgentFactory, contractLockingScript bitcoin.Script,
	transferTxID bitcoin.Hash32) error {

	expirationTime := time.Unix(0, int64(startTimestamp))
	task := agents.NewCancelPendingTransferTask(expirationTime, factory, contractLockingScript,
		transferTxID, startTimestamp)

	scheduler.Schedule(ctx, task)
	return nil
}
