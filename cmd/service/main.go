package main

import (
	"context"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"

	"github.com/tokenized/config"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/service"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	"github.com/tokenized/smart_contract_agent/pkg/contract_services"
	"github.com/tokenized/smart_contract_agent/pkg/headers"
	"github.com/tokenized/smart_contract_agent/pkg/locker"
	"github.com/tokenized/smart_contract_agent/pkg/scheduler"
	"github.com/tokenized/smart_contract_agent/pkg/statistics"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	spyNodeClient "github.com/tokenized/spynode/pkg/client"
	"github.com/tokenized/threads"
)

var (
	buildVersion = "unknown"
	buildDate    = "unknown"
	buildUser    = "unknown"
)

type Config struct {
	AgentData                   agents.AgentData `json:"agent_data"`
	Agents                      agents.Config    `json:"agents"`
	FeeAddress                  bitcoin.Address  `envconfig:"FEE_ADDRESS" json:"fee_address"`
	RequestPeerChannelReadToken *string          `envconfig:"REQUEST_PEER_CHANNEL_READ_TOKEN" json:"request_peer_channel_read_token"`
	ChannelTimeout              config.Duration  `default:"1s" envconfig:"CHANNEL_TIMEOUT" json:"channel_timeout"`

	Storage storage.Config       `json:"storage"`
	SpyNode spyNodeClient.Config `json:"spynode"`
	Logger  logger.SetupConfig   `json:"logger"`
}

func main() {
	// Logging
	ctx := context.Background()

	cfg := Config{}
	if err := config.LoadConfig(ctx, &cfg); err != nil {
		logger.Fatal(ctx, "LoadConfig : %s", err)
	}

	ctx = logger.ContextWithLogSetup(ctx, cfg.Logger)

	logger.Info(ctx, "Starting %s : Build %s (%s on %s)", "Smart Contract Agent", buildVersion,
		buildUser, buildDate)
	defer logger.Info(ctx, "Completed")
	defer func() {
		if err := recover(); err != nil {
			logger.Error(ctx, "Panic : %s : %s", err, string(debug.Stack()))
		}
	}()

	// Config
	maskedConfig, err := config.MarshalJSONMaskedRaw(cfg)
	if err != nil {
		logger.Fatal(ctx, "Failed to marshal config : %s", err)
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.JSON("config", maskedConfig),
	}, "Config")

	store, err := storage.CreateStreamStorage(cfg.Storage.Bucket, cfg.Storage.Root,
		cfg.Storage.MaxRetries, cfg.Storage.RetryDelay)
	if err != nil {
		logger.Fatal(ctx, "Failed to create storage : %s", err)
	}

	peerChannelsFactory := peer_channels.NewFactory()

	peerChannelResponses := make(chan agents.PeerChannelResponse, 100)

	if cfg.SpyNode.ConnectionType != spyNodeClient.ConnectionTypeFull {
		logger.Fatal(ctx, "Spynode connection type must be full to receive data : %s", err)
	}

	spyNode, err := spyNodeClient.NewRemoteClient(&cfg.SpyNode)
	if err != nil {
		logger.Fatal(ctx, "Failed to create spynode remote client : %s", err)
	}

	broadcaster := NewSpyNodeBroadcaster(spyNode)

	scheduler := scheduler.NewScheduler(broadcaster, cfg.ChannelTimeout.Duration)

	cache := cacher.NewSimpleCache(store)
	caches, err := state.NewCaches(cache)
	if err != nil {
		logger.Fatal(ctx, "Failed to create caches : %s", err)
	}

	transactions, err := transactions.NewTransactionCache(cache)
	if err != nil {
		logger.Fatal(ctx, "Failed to create transactions cache : %s", err)
	}

	services, err := contract_services.NewContractServicesCache(cache)
	if err != nil {
		logger.Fatal(ctx, "Failed to create services cache : %s", err)
	}

	statistics, err := statistics.NewProcessor(cache, 100, cfg.ChannelTimeout.Duration)
	if err != nil {
		logger.Fatal(ctx, "Failed to create statistics processor : %s", err)
	}

	locker := locker.NewThreadedLocker(1000)

	dependencyTrigger := agents.NewDependencyTrigger(1, cfg.ChannelTimeout.Duration, transactions)

	service := service.NewService(cfg.AgentData, cfg.Agents, spyNode, caches, transactions,
		services, locker, store, broadcaster, spyNode, headers.NewHeaders(spyNode), scheduler,
		peerChannelsFactory, peerChannelResponses, statistics, dependencyTrigger.Trigger)
	spyNode.RegisterHandler(service)

	var spyNodeWait, lockerWait, peerChannelResponseWait, schedulerWait,
		peerChannelWait, statisticsWait, dependencyTriggerWait sync.WaitGroup

	schedulerThread, schedulerComplete := threads.NewInterruptableThreadComplete("Scheduler",
		scheduler.Run, &schedulerWait)

	lockerThread, lockerComplete := threads.NewInterruptableThreadComplete("Balance Locker",
		locker.Run, &lockerWait)

	peerChannelResponder := agents.NewPeerChannelResponder(caches, peerChannelsFactory)
	peerChannelResponseThread, peerChannelResponseComplete := threads.NewUninterruptableThreadComplete("Peer Channel Response",
		func(ctx context.Context) error {
			return agents.ProcessResponses(ctx, peerChannelResponder, peerChannelResponses)
		}, &peerChannelResponseWait)

	spyNodeThread, spyNodeComplete := threads.NewInterruptableThreadComplete("SpyNode", spyNode.Run,
		&spyNodeWait)

	statisticsThread, statisticsComplete := threads.NewInterruptableThreadComplete("Statistics",
		statistics.Run, &statisticsWait)

	peerChannelThread, peerChannelComplete := threads.NewInterruptableThreadComplete("Peer Channel Listen",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return service.PeerChannelListen(ctx, interrupt, cfg.AgentData.RequestPeerChannel,
				cfg.RequestPeerChannelReadToken)
		}, &peerChannelWait)

	dependencyTriggerThread, dependencyTriggerComplete := threads.NewInterruptableThreadComplete("Dependency Trigger",
		dependencyTrigger.Run, &dependencyTriggerWait)

	if err := service.Load(ctx); err != nil {
		service.ReleaseAll(ctx)
		logger.Fatal(ctx, "Failed to load service : %s", err)
	}

	schedulerThread.Start(ctx)
	lockerThread.Start(ctx)
	peerChannelResponseThread.Start(ctx)
	dependencyTriggerThread.Start(ctx)
	spyNodeThread.Start(ctx)
	statisticsThread.Start(ctx)
	peerChannelThread.Start(ctx)

	// Shutdown
	//
	// Make a channel to listen for an interrupt or terminate signal from the OS.
	// Use a buffered channel because the signal package requires it.
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM)

	// Stop API Service
	//
	// Blocking main and waiting for shutdown.
	select {
	case err := <-schedulerComplete:
		logger.Error(ctx, "Scheduler shutting down : %s", err)

	case err := <-lockerComplete:
		logger.Error(ctx, "Balance locker completed : %s", err)

	case err := <-peerChannelResponseComplete:
		logger.Error(ctx, "Peer Channel Response completed : %s", err)

	case err := <-spyNodeComplete:
		logger.Error(ctx, "SpyNode completed : %s", err)

	case err := <-statisticsComplete:
		logger.Error(ctx, "Statistics completed : %s", err)

	case err := <-dependencyTriggerComplete:
		logger.Error(ctx, "Dependency Trigger completed : %s", err)

	case err := <-peerChannelComplete:
		logger.Error(ctx, "Peer Channel Listen completed : %s", err)

	case <-osSignals:
		logger.Info(ctx, "Start shutdown")
	}

	peerChannelThread.Stop(ctx)
	peerChannelWait.Wait()

	spyNodeThread.Stop(ctx)
	spyNodeWait.Wait()

	dependencyTriggerThread.Stop(ctx)
	dependencyTriggerWait.Wait()

	statisticsThread.Stop(ctx)
	statisticsWait.Wait()

	close(peerChannelResponses)
	peerChannelResponseWait.Wait()

	lockerThread.Stop(ctx)
	lockerWait.Wait()

	if err := service.Save(ctx); err != nil {
		logger.Error(ctx, "Failed to save service : %s", err)
	}

	// This needs to be stopped after the service save so that the service save can list the tasks.
	schedulerThread.Stop(ctx)
	schedulerWait.Wait()

	service.ReleaseAll(ctx)
}

type SpyNodeBroadcaster struct {
	client spyNodeClient.Client
}

func NewSpyNodeBroadcaster(client spyNodeClient.Client) *SpyNodeBroadcaster {
	return &SpyNodeBroadcaster{
		client: client,
	}
}

func (b *SpyNodeBroadcaster) BroadcastTx(ctx context.Context, etx *expanded_tx.ExpandedTx,
	indexes []uint32) error {
	return b.client.SendExpandedTxAndMarkOutputs(ctx, etx, indexes)
}
