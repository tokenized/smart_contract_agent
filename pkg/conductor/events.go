package conductor

import (
	"context"
	"time"

	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/agents"

	"github.com/pkg/errors"
)

func ScheduleEvents(ctx context.Context, store storage.StreamStorage, caches *state.Caches,
	scheduler *platform.Scheduler, factory agents.AgentFactory) error {

	events, err := state.ListEvents(ctx, store)
	if err != nil {
		return errors.Wrap(err, "list events")
	}

	for _, event := range events {
		switch event.Type {
		case state.EventTypeVoteCutOff:
			vote, err := caches.Votes.Get(ctx, event.ContractLockingScript, event.ID)
			if err != nil {
				return errors.Wrap(err, "get vote")
			}

			if vote == nil {
				return errors.New("Vote not found")
			}

			if vote.Proposal == nil {
				caches.Votes.Release(ctx, event.ContractLockingScript, event.ID)
				return errors.New("Vote missing proposal")
			}

			cutOffTime := time.Unix(0, int64(vote.Proposal.VoteCutOffTimestamp))

			scheduler.Schedule(ctx, event.ID, cutOffTime,
				func(ctx context.Context, interrupt <-chan interface{}) error {
					return agents.FinalizeVote(ctx, factory, event.ContractLockingScript, event.ID,
						vote.Proposal.VoteCutOffTimestamp)
				})

			caches.Votes.Release(ctx, event.ContractLockingScript, event.ID)

		case state.EventTypeTransferExpiration:

		}
	}

	return nil
}

func SaveEvents(ctx context.Context, store storage.StreamStorage, caches *state.Caches,
	scheduler *platform.Scheduler) error {

	// // Extract events from scheduler

	// if err := state.SaveEvents(ctx, store, events); err != nil {
	// 	return errors.Wrap(err, "save")
	// }

	return nil
}
