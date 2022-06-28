package agents

import (
	"context"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/specification/dist/golang/actions"
)

func (a *Agent) processVote(ctx context.Context, transaction TransactionWithOutputs,
	index int, vote *actions.Vote) error {

	if index != 0 {
		logger.Warn(ctx, "Vote not from input zero: %d", index)
		return nil
	}

	logger.Info(ctx, "Processing vote")

	return nil
}

func (a *Agent) processBallotCounted(ctx context.Context, transaction TransactionWithOutputs,
	index int, ballotCounted *actions.BallotCounted) error {

	if index != 0 {
		logger.Warn(ctx, "Ballot counted not from input zero: %d", index)
		return nil
	}

	logger.Info(ctx, "Processing ballot counted")

	return nil
}

func (a *Agent) processGovernanceResult(ctx context.Context, transaction TransactionWithOutputs,
	index int, result *actions.Result) error {

	if index != 0 {
		logger.Warn(ctx, "Governance result counted not from input zero: %d", index)
		return nil
	}

	logger.Info(ctx, "Processing governance result")

	return nil
}
