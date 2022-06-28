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
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/firm"
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

	CacheFetcherCount int             `default:"10" envconfig:"CACHE_FETCHER_COUNT" json:"cache_fetcher_count"`
	CacheExpireCount  int             `default:"1000" envconfig:"CACHE_EXPIRE_COUNT" json:"cache_expire_count"`
	CacheExpiration   config.Duration `default:"20s" envconfig:"CACHE_EXPIRATION" json:"cache_expiration"`
	CacheFetchTimeout config.Duration `default:"1m" envconfig:"CACHE_FETCH_TIMEOUT" json:"cache_fetch_timeout"`

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

	contracts, err := state.NewContractCache(store, cfg.CacheFetcherCount, cfg.CacheExpireCount,
		cfg.CacheExpiration.Duration, cfg.CacheFetchTimeout.Duration)
	if err != nil {
		logger.Fatal(ctx, "main : Failed to create contracts cache : %s", err)
	}

	balances, err := state.NewBalanceCache(store, cfg.CacheFetcherCount, cfg.CacheExpireCount,
		cfg.CacheExpiration.Duration, cfg.CacheFetchTimeout.Duration)
	if err != nil {
		logger.Fatal(ctx, "main : Failed to create balance cache : %s", err)
	}

	transactions, err := state.NewTransactionCache(store, cfg.CacheFetcherCount, cfg.CacheExpireCount,
		cfg.CacheExpiration.Duration, cfg.CacheFetchTimeout.Duration)
	if err != nil {
		logger.Fatal(ctx, "main : Failed to create transaction cache : %s", err)
	}

	firm := firm.NewFirm(cfg.BaseKey, cfg.IsTest, spyNodeClient, contracts, balances, transactions)
	spyNodeClient.RegisterHandler(firm)

	var wait sync.WaitGroup

	spynodeErrors := make(chan error, 10)
	spyNodeClient.SetListenerErrorChannel(&spynodeErrors)

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
		logger.Error(ctx, "Spynode failed : %s", err)

	case <-osSignals:
		logger.Info(ctx, "main : Start shutdown...")
	}

	spyNodeClient.Close(ctx) // This waits for spynode to finish

	wait.Wait()
}