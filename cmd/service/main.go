package main

import (
	"context"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"

	"github.com/tokenized/cacher"
	"github.com/tokenized/config"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	"github.com/tokenized/smart_contract_agent/pkg/service"
	spyNodeClient "github.com/tokenized/spynode/pkg/client"
	"github.com/tokenized/threads"
)

var (
	buildVersion = "unknown"
	buildDate    = "unknown"
	buildUser    = "unknown"
)

type Config struct {
	AgentKey   bitcoin.Key     `envconfig:"AGENT_KEY" json:"agent_key" masked:"true"`
	Agents     agents.Config   `json:"agents"`
	FeeAddress bitcoin.Address `envconfig:"FEE_ADDRESS" json:"fee_address"`

	Storage storage.Config       `json:"storage"`
	Cache   cacher.Config        `json:"cache"`
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

	feeLockingScript, err := bitcoin.NewRawAddressFromAddress(cfg.FeeAddress).LockingScript()
	if err != nil {
		logger.Fatal(ctx, "Invalid fee address : %s", err)
	}

	store, err := storage.CreateStreamStorage(cfg.Storage.Bucket, cfg.Storage.Root,
		cfg.Storage.MaxRetries, cfg.Storage.RetryDelay)
	if err != nil {
		logger.Fatal(ctx, "Failed to create storage : %s", err)
	}

	scheduler := platform.NewScheduler()

	if cfg.SpyNode.ConnectionType != spyNodeClient.ConnectionTypeFull {
		logger.Fatal(ctx, "Spynode connection type must be full to receive data : %s", err)
	}

	spyNode, err := spyNodeClient.NewRemoteClient(&cfg.SpyNode)
	if err != nil {
		logger.Fatal(ctx, "Failed to create spynode remote client : %s", err)
	}

	cache := cacher.NewCache(store, cfg.Cache)
	caches, err := state.NewCaches(cache)
	if err != nil {
		logger.Fatal(ctx, "Failed to create caches : %s", err)
	}

	lockingScript, err := cfg.AgentKey.LockingScript()
	if err != nil {
		logger.Fatal(ctx, "Failed to create agent locking script : %s", err)
	}

	service := service.NewService(cfg.AgentKey, lockingScript, cfg.Agents, feeLockingScript,
		spyNode, caches, store, NewSpyNodeBroadcaster(spyNode), spyNode,
		platform.NewHeaders(spyNode), scheduler)
	spyNode.RegisterHandler(service)

	if err := service.Load(ctx); err != nil {
		logger.Fatal(ctx, "Failed to load service : %s", err)
	}

	var spyNodeWait, cacheWait, schedulerWait sync.WaitGroup

	schedulerThread, schedulerComplete := threads.NewInterruptableThreadComplete("Scheduler",
		scheduler.Run, &schedulerWait)

	cacheShutdown := make(chan error)
	cacheThread, cacheComplete := threads.NewInterruptableThreadComplete("Cache",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return cache.Run(ctx, interrupt, cacheShutdown)
		}, &cacheWait)

	spyNodeThread, spyNodeComplete := threads.NewInterruptableThreadComplete("SpyNode", spyNode.Run,
		&spyNodeWait)

	schedulerThread.Start(ctx)
	cacheThread.Start(ctx)
	spyNodeThread.Start(ctx)

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

	case err := <-cacheShutdown:
		logger.Error(ctx, "Cache shutting down : %s", err)

	case err := <-cacheComplete:
		logger.Error(ctx, "Cache completed : %s", err)

	case err := <-spyNodeComplete:
		logger.Error(ctx, "SpyNode completed : %s", err)

	case <-osSignals:
		logger.Info(ctx, "Start shutdown")
	}

	schedulerThread.Stop(ctx)
	schedulerWait.Wait()

	spyNodeThread.Stop(ctx)
	spyNodeWait.Wait()

	if err := service.Save(ctx); err != nil {
		logger.Error(ctx, "Failed to save service : %s", err)
	}

	service.Release(ctx)

	cacheThread.Stop(ctx)
	cacheWait.Wait()
}

type SpyNodeBroadcaster struct {
	client spyNodeClient.Client
}

func NewSpyNodeBroadcaster(client spyNodeClient.Client) *SpyNodeBroadcaster {
	return &SpyNodeBroadcaster{
		client: client,
	}
}

func (b *SpyNodeBroadcaster) BroadcastTx(ctx context.Context, tx *wire.MsgTx,
	indexes []uint32) error {
	return b.client.SendTxAndMarkOutputs(ctx, tx, indexes)
}
