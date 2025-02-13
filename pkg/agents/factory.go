package agents

import (
	"context"

	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/contract_services"
	"github.com/tokenized/smart_contract_agent/pkg/locker"
	"github.com/tokenized/smart_contract_agent/pkg/scheduler"
	"github.com/tokenized/smart_contract_agent/pkg/statistics"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"

	"github.com/pkg/errors"
)

type Factory struct {
	config               Config
	store                storage.CopyList
	caches               *state.Caches
	transactions         *transactions.TransactionCache
	services             *contract_services.ContractServicesCache
	locker               locker.Locker
	broadcaster          Broadcaster
	fetcher              Fetcher
	headers              BlockHeaders
	scheduler            *scheduler.Scheduler
	agentStore           Store
	peerChannelsFactory  *peer_channels.Factory
	peerChannelResponses chan PeerChannelResponse
	updateStats          statistics.AddUpdate
	triggerDependency    TriggerDependency
}

func NewFactory(config Config, store storage.CopyList, cache cacher.Cacher,
	transactions *transactions.TransactionCache,
	services *contract_services.ContractServicesCache, locker locker.Locker,
	broadcaster Broadcaster, fetcher Fetcher, headers BlockHeaders, scheduler *scheduler.Scheduler,
	agentStore Store, peerChannelsFactory *peer_channels.Factory,
	peerChannelResponses chan PeerChannelResponse,
	updateStats statistics.AddUpdate, triggerDependency TriggerDependency) (*Factory, error) {

	caches, err := state.NewCaches(cache)
	if err != nil {
		return nil, errors.Wrap(err, "caches")
	}

	return &Factory{
		config:               config,
		store:                store,
		caches:               caches,
		transactions:         transactions,
		services:             services,
		locker:               locker,
		broadcaster:          broadcaster,
		fetcher:              fetcher,
		headers:              headers,
		scheduler:            scheduler,
		agentStore:           agentStore,
		peerChannelsFactory:  peerChannelsFactory,
		peerChannelResponses: peerChannelResponses,
		updateStats:          updateStats,
		triggerDependency:    triggerDependency,
	}, nil
}

func (f *Factory) NewAgent(ctx context.Context, data AgentData) (*Agent, error) {
	return NewAgent(ctx, data, f.config, f.caches, f.transactions, f.services, f.locker, f.store,
		f.broadcaster, f.fetcher, f.headers, f.scheduler, f.agentStore, f.peerChannelsFactory,
		f.peerChannelResponses, f.updateStats, f.triggerDependency)
}

func (f *Factory) NewPeerChannelResponder() *PeerChannelResponder {
	return NewPeerChannelResponder(f.caches, f.peerChannelsFactory)
}
