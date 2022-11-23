package agents

import (
	"context"
	"sync"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"

	"github.com/pkg/errors"
)

type MockAgentFactory struct {
	keys []*bitcoin.Key

	config           Config
	feeLockingScript bitcoin.Script
	caches           *state.Caches
	store            storage.CopyList
	broadcaster      Broadcaster
	fetcher          Fetcher
	headers          BlockHeaders
	scheduler        *platform.Scheduler

	lock sync.Mutex
}

func NewMockAgentFactory(config Config, feeLockingScript bitcoin.Script, caches *state.Caches,
	store storage.CopyList, broadcaster Broadcaster, fetcher Fetcher, headers BlockHeaders,
	scheduler *platform.Scheduler) *MockAgentFactory {

	return &MockAgentFactory{
		config:      config,
		caches:      caches,
		store:       store,
		fetcher:     fetcher,
		broadcaster: broadcaster,
		scheduler:   scheduler,
	}
}

func (f *MockAgentFactory) AddKey(key bitcoin.Key) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.keys = append(f.keys, &key)
}

func (f *MockAgentFactory) GetAgent(ctx context.Context,
	lockingScript bitcoin.Script) (*Agent, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	var key *bitcoin.Key
	for _, k := range f.keys {
		ls, err := k.LockingScript()
		if err != nil {
			continue
		}

		if ls.Equal(lockingScript) {
			key = k
			break
		}
	}

	if key == nil {
		return nil, nil
	}

	agent, err := NewAgent(ctx, *key, lockingScript, f.config, f.feeLockingScript, f.caches,
		f.store, f.broadcaster, f.fetcher, f.headers, f.scheduler, f)
	if err != nil {
		return nil, errors.Wrap(err, "new agent")
	}

	return agent, nil
}
