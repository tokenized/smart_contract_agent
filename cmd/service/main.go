package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/config"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/threads"
	"github.com/tokenized/smart_contract_agent/internal/cacher"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/conductor"
	spyNodeClient "github.com/tokenized/spynode/pkg/client"
)

var (
	buildVersion = "unknown"
	buildDate    = "unknown"
	buildUser    = "unknown"
)

type Config struct {
	BaseKey bitcoin.Key `envconfig:"BASE_KEY" json:"base_key" masked:"true"`

	IsTest bool `default:"true" envconfig:"IS_TEST" json:"is_test"`

	CacheRequestThreadCount int             `default:"4" envconfig:"CACHE_REQUEST_THREAD_COUNT" json:"cache_request_thread_count"`
	CacheRequestTimeout     config.Duration `default:"1m" envconfig:"CACHE_REQUEST_TIMEOUT" json:"cache_request_timeout"`
	CacheExpireCount        int             `default:"10000" envconfig:"CACHE_EXPIRE_COUNT" json:"cache_expire_count"`
	CacheExpiration         config.Duration `default:"3s" envconfig:"CACHE_EXPIRATION" json:"cache_expiration"`

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

	spyNodeClient, err := spyNodeClient.NewRemoteClient(&cfg.SpyNode)
	if err != nil {
		logger.Fatal(ctx, "main : Failed to create spynode remote client : %s", err)
	}

	cache := cacher.NewCache(store, cfg.CacheRequestThreadCount, cfg.CacheRequestTimeout.Duration,
		cfg.CacheExpireCount, cfg.CacheExpiration.Duration)

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

	conductor := conductor.NewConductor(cfg.BaseKey, cfg.IsTest, spyNodeClient, contracts, balances,
		transactions, subscriptions)
	spyNodeClient.RegisterHandler(conductor)

	var wait sync.WaitGroup
	var stopper threads.StopCombiner

	spynodeErrors := make(chan error, 10)
	spyNodeClient.SetListenerErrorChannel(&spynodeErrors)

	cacheThread := threads.NewThread("Cache", cache.Run)
	cacheThread.SetWait(&wait)
	cacheComplete := cacheThread.GetCompleteChannel()
	stopper.Add(cacheThread)

	cacheThread.Start(ctx)

	if err := spyNodeClient.Connect(ctx); err != nil {
		logger.Fatal(ctx, "main : Failed to connect to spynode : %s", err)
	}

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
	case err := <-spynodeErrors:
		logger.Error(ctx, "main : Spynode failed : %s", err)

	case <-cacheComplete:
		logger.Error(ctx, "main : Cache thread completed : %s", cacheThread.Error())

	case <-osSignals:
		logger.Info(ctx, "main : Start shutdown...")
	}

	// This waits for spynode to finish which must happen before stopping the caches.
	spyNodeClient.Close(ctx)
	stopper.Stop(ctx)

	wait.Wait()

	if err := conductor.Save(ctx, store); err != nil {
		logger.Error(ctx, "main : Failed to save conductor : %s", err)
	}
}
