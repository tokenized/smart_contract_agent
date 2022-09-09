package main

import (
	"context"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"

	"github.com/tokenized/cacher"
	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/config"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/conductor"
	spyNodeClient "github.com/tokenized/spynode/pkg/client"
	"github.com/tokenized/threads"
)

var (
	buildVersion = "unknown"
	buildDate    = "unknown"
	buildUser    = "unknown"
)

type Config struct {
	BaseKey bitcoin.Key `envconfig:"BASE_KEY" json:"base_key" masked:"true"`

	IsTest bool `default:"true" envconfig:"IS_TEST" json:"is_test"`

	Cache cacher.Config `json:"cache"`

	Wallet  wallet.Config        `json:"wallet"`
	Storage storage.Config       `json:"storage"`
	SpyNode spyNodeClient.Config `json:"spynode"`
	Logger  logger.SetupConfig   `json:"logger"`
}

func main() {
	// Logging
	ctx := context.Background()

	cfg := Config{}
	if err := config.LoadConfig(ctx, &cfg); err != nil {
		logger.Fatal(ctx, "main : LoadConfig : %s", err)
	}

	ctx = logger.ContextWithLogSetup(ctx, cfg.Logger)

	logger.Info(ctx, "Starting %s : Build %s (%s on %s)", "Smart Contract Agent", buildVersion,
		buildUser, buildDate)
	defer logger.Info(ctx, "main : Completed")
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
		logger.Fatal(ctx, "main : Failed to create storage : %s", err)
	}

	if cfg.SpyNode.ConnectionType != spyNodeClient.ConnectionTypeFull {
		logger.Fatal(ctx, "main : Spynode connection type must be full to receive data : %s", err)
	}

	spyNode, err := spyNodeClient.NewRemoteClient(&cfg.SpyNode)
	if err != nil {
		logger.Fatal(ctx, "main : Failed to create spynode remote client : %s", err)
	}

	cache := cacher.NewCache(store, cfg.Cache)

	contracts, err := state.NewContractCache(cache)
	if err != nil {
		logger.Fatal(ctx, "main : Failed to create contracts cache : %s", err)
	}

	balances, err := state.NewBalanceCache(cache)
	if err != nil {
		logger.Fatal(ctx, "main : Failed to create balance cache : %s", err)
	}

	transactions, err := state.NewTransactionCache(cache)
	if err != nil {
		logger.Fatal(ctx, "main : Failed to create transaction cache : %s", err)
	}

	subscriptions, err := state.NewSubscriptionCache(cache)
	if err != nil {
		logger.Fatal(ctx, "main : Failed to create subscription cache : %s", err)
	}

	conductor := conductor.NewConductor(cfg.BaseKey, cfg.IsTest, spyNode, contracts, balances,
		transactions, subscriptions)
	spyNode.RegisterHandler(conductor)

	var spyNodeWait, cacheWait sync.WaitGroup

	cacheThread, cacheComplete := threads.NewInterruptableThreadComplete("Cache", cache.Run,
		&cacheWait)

	spyNodeThread, spyNodeComplete := threads.NewInterruptableThreadComplete("SpyNode", spyNode.Run,
		&spyNodeWait)

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
	case err := <-cacheComplete:
		logger.Error(ctx, "Cache completed : %s", err)

	case err := <-spyNodeComplete:
		logger.Error(ctx, "SpyNode completed : %s", err)

	case <-osSignals:
		logger.Info(ctx, "Start shutdown")
	}

	spyNodeThread.Stop(ctx)
	spyNodeWait.Wait()

	if err := conductor.Save(ctx, store); err != nil {
		logger.Error(ctx, "main : Failed to save conductor : %s", err)
	}

	cacheThread.Stop(ctx)
	cacheWait.Wait()
}
