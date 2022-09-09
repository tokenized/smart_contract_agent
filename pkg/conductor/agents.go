package conductor

import (
	"context"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	"github.com/tokenized/specification/dist/golang/actions"

	"github.com/pkg/errors"
)

type AgentActions struct {
	agent   *agents.Agent
	actions []actions.Action
}

type AgentActionsList []*AgentActions

func (c *Conductor) addAgentAction(ctx context.Context, agentActionsList *AgentActionsList,
	agentLockingScript bitcoin.Script, action actions.Action) error {

	for _, agentActions := range *agentActionsList {
		if agentLockingScript.Equal(agentActions.agent.LockingScript()) {
			agentActions.actions = append(agentActions.actions, action)
			return nil
		}
	}

	agent, err := c.GetAgent(ctx, agentLockingScript)
	if err != nil {
		return errors.Wrap(err, "get agent")
	}

	if agent == nil {
		logger.Verbose(ctx, "Agent not found for locking script : %s", agentLockingScript)
		return nil
	}

	*agentActionsList = append(*agentActionsList, &AgentActions{
		agent:   agent,
		actions: []actions.Action{action},
	})
	return nil
}

func (c *Conductor) releaseAgentActions(ctx context.Context, agentActionsList AgentActionsList) {
	for _, agentAction := range agentActionsList {
		c.ReleaseAgent(ctx, agentAction.agent)
	}
}
