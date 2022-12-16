package agents

import (
	"context"

	"github.com/tokenized/cacher"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/contract_services"
	"github.com/tokenized/smart_contract_agent/pkg/locker"
	"github.com/tokenized/smart_contract_agent/pkg/scheduler"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"

	"github.com/pkg/errors"
)

type Factory struct {
	config              Config
	store               storage.CopyList
	caches              *state.Caches
	transactions        *transactions.TransactionCache
	services            *contract_services.ContractServicesCache
	locker              locker.Locker
	broadcaster         Broadcaster
	fetcher             Fetcher
	headers             BlockHeaders
	scheduler           *scheduler.Scheduler
	agentStore          Store
	peerChannelsFactory *peer_channels.Factory
}

func NewFactory(config Config, store storage.CopyList, cache *cacher.Cache,
	transactions *transactions.TransactionCache,
	services *contract_services.ContractServicesCache, locker locker.Locker,
	broadcaster Broadcaster, fetcher Fetcher, headers BlockHeaders, scheduler *scheduler.Scheduler,
	agentStore Store, peerChannelsFactory *peer_channels.Factory) (*Factory, error) {

	caches, err := state.NewCaches(cache)
	if err != nil {
		return nil, errors.Wrap(err, "caches")
	}

	return &Factory{
		config:              config,
		store:               store,
		caches:              caches,
		transactions:        transactions,
		services:            services,
		locker:              locker,
		broadcaster:         broadcaster,
		fetcher:             fetcher,
		headers:             headers,
		scheduler:           scheduler,
		agentStore:          agentStore,
		peerChannelsFactory: peerChannelsFactory,
	}, nil
}

func (f *Factory) NewAgent(ctx context.Context, key bitcoin.Key, lockingScript bitcoin.Script,
	feeLockingScript bitcoin.Script) (*Agent, error) {
	return NewAgent(ctx, key, lockingScript, f.config, feeLockingScript, f.caches, f.transactions,
		f.services, f.locker, f.store, f.broadcaster, f.fetcher, f.headers, f.scheduler,
		f.agentStore, f.peerChannelsFactory)
}
