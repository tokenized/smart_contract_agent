package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/tokenized/cacher"
	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/config"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/internal/whatsonchain"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	"github.com/tokenized/smart_contract_agent/pkg/conductor"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

var (
	buildVersion = "unknown"
	buildDate    = "unknown"
	buildUser    = "unknown"
)

type Config struct {
	BaseKey bitcoin.Key `envconfig:"BASE_KEY" json:"base_key" masked:"true"`

	Cache cacher.Config `json:"cache"`

	Wallet  wallet.Config      `json:"wallet"`
	Storage storage.Config     `json:"storage"`
	Logger  logger.SetupConfig `json:"logger"`

	ContractKey        bitcoin.Key `envconfig:"CONTRACT_KEY" json:"contract_key" masked:"true"`
	WhatsOnChainAPIKey string      `envconfig:"WOC_API_KEY" json:"api_key" masked:"true"`

	Network bitcoin.Network `default:"mainnet" json:"network" envconfig:"NETWORK"`

	FeeAddress bitcoin.Address `envconfig:"FEE_ADDRESS" json:"fee_address"`

	Agents agents.Config `json:"agents"`
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

	state.SetIsTest(cfg.Agents.IsTest)

	feeLockingScript, err := bitcoin.NewRawAddressFromAddress(cfg.FeeAddress).LockingScript()
	if err != nil {
		logger.Fatal(ctx, "Invalid fee address : %s", err)
	}

	woc := whatsonchain.NewService(cfg.WhatsOnChainAPIKey, cfg.Network)

	store, err := storage.CreateStreamStorage(cfg.Storage.Bucket, cfg.Storage.Root,
		cfg.Storage.MaxRetries, cfg.Storage.RetryDelay)
	if err != nil {
		logger.Fatal(ctx, "main : Failed to create storage : %s", err)
	}

	cache := cacher.NewCache(store, cfg.Cache)
	caches, err := state.NewCaches(cache)
	if err != nil {
		logger.Fatal(ctx, "main : Failed to create caches : %s", err)
	}

	broadcaster := NewNoopBroadcaster()

	conductor := conductor.NewConductor(cfg.BaseKey, cfg.Agents, feeLockingScript, nil, caches,
		store, broadcaster, woc, woc)

	var cacheWait, loadWait sync.WaitGroup

	cacheShutdown := make(chan error)
	cacheThread, cacheComplete := threads.NewInterruptableThreadComplete("Cache",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return cache.Run(ctx, interrupt, cacheShutdown)
		}, &cacheWait)

	loadThread, loadComplete := threads.NewInterruptableThreadComplete("Load",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return load(ctx, interrupt, conductor, caches.Transactions, cfg.BaseKey,
				cfg.ContractKey, woc)
		}, &loadWait)

	cacheThread.Start(ctx)

	if err := conductor.Load(ctx, store); err != nil {
		logger.Fatal(ctx, "main : Failed to load conductor : %s", err)
	}

	loadThread.Start(ctx)

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
	case err := <-cacheShutdown:
		logger.Error(ctx, "Cache shutting down : %s", err)

	case err := <-cacheComplete:
		logger.Error(ctx, "Cache completed : %s", err)

	case err := <-loadComplete:
		if err != nil {
			logger.Error(ctx, "Load completed : %s", err)
		} else {
			logger.Info(ctx, "Load completed")
		}

	case <-osSignals:
		logger.Info(ctx, "main : Start shutdown...")
	}

	loadThread.Stop(ctx)
	loadWait.Wait()

	if err := conductor.Save(ctx, store); err != nil {
		logger.Error(ctx, "main : Failed to save conductor : %s", err)
	}

	cacheThread.Stop(ctx)
	cacheWait.Wait()
}

func getTx(ctx context.Context, transactions *state.TransactionCache, woc *whatsonchain.Service,
	txid bitcoin.Hash32) (*state.Transaction, error) {

	if transaction, err := transactions.Get(ctx, txid); err != nil {
		return nil, errors.Wrapf(err, "get transaction: %s", txid)
	} else if transaction != nil {
		return transaction, nil
	}

	logger.Info(ctx, "Fetching transaction")
	gotTx, err := woc.GetTx(ctx, txid)
	if err != nil {
		return nil, errors.Wrapf(err, "woc get tx: %s", txid)
	}

	newTransaction := &state.Transaction{
		Tx:    gotTx,
		State: wallet.TxStateSafe, // assume safe because it is history
	}
	return transactions.Add(ctx, newTransaction)
}

func getInputs(ctx context.Context, transactions *state.TransactionCache,
	woc *whatsonchain.Service, transaction *state.Transaction) error {

	transaction.Lock()
	for index, txin := range transaction.Tx.TxIn {
		if _, err := transaction.InputOutput(index); err == nil {
			continue
		}

		inputTx, err := getTx(ctx, transactions, woc, txin.PreviousOutPoint.Hash)
		if err != nil {
			transaction.Unlock()
			return errors.Wrapf(err, "load input tx: %s", txin.PreviousOutPoint.Hash)
		}

		inputTx.Lock()
		transaction.Ancestors = append(transaction.Ancestors, &expanded_tx.AncestorTx{
			Tx: inputTx.Tx,
		})
		inputTx.Unlock()
		transactions.Release(ctx, txin.PreviousOutPoint.Hash)
	}
	transaction.Unlock()

	return nil
}

func loadTx(ctx context.Context, conductor *conductor.Conductor,
	transactions *state.TransactionCache, woc *whatsonchain.Service, txid bitcoin.Hash32) error {
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("txid", txid))

	transaction, err := getTx(ctx, transactions, woc, txid)
	if err != nil {
		return errors.Wrapf(err, "get transaction: %s", txid)
	}
	defer transactions.Release(ctx, txid)

	if err := getInputs(ctx, transactions, woc, transaction); err != nil {
		return errors.Wrapf(err, "get inputs: %s", txid)
	}

	if err := conductor.UpdateTransaction(ctx, transaction,
		uint64(time.Now().UnixNano())); err != nil {
		return errors.Wrapf(err, "update transaction")
	}

	return nil
}

func load(ctx context.Context, interrupt <-chan interface{}, conductor *conductor.Conductor,
	transactions *state.TransactionCache, baseKey, contractKey bitcoin.Key,
	woc *whatsonchain.Service) error {

	hash, err := contractKey.Subtract(baseKey)
	if err != nil {
		return errors.Wrap(err, "subtract key")
	}

	lockingScript, err := contractKey.LockingScript()
	if err != nil {
		return errors.Wrap(err, "locking script")
	}

	agent, err := conductor.AddAgent(ctx, hash)
	if err != nil {
		return errors.Wrap(err, "add agent")
	}
	defer conductor.ReleaseAgent(ctx, agent)

	history, err := woc.GetLockingScriptHistory(ctx, lockingScript)
	if err != nil {
		return errors.Wrap(err, "get history")
	}

	select {
	case <-interrupt:
		return nil
	default:
	}

	index := 0
	for {
		if index >= len(history) {
			return nil
		}

		txid := history[index].TxID
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txid),
		}, "Loading transaction")

		if err := loadTx(ctx, conductor, transactions, woc, history[index].TxID); err != nil {
			return errors.Wrapf(err, "load transaction: %s", txid)
		}

		select {
		case <-interrupt:
			return nil
		default:
		}

		index++
	}
}

type NoopBroadcaster struct{}

func NewNoopBroadcaster() *NoopBroadcaster {
	return &NoopBroadcaster{}
}

func (*NoopBroadcaster) BroadcastTx(ctx context.Context, tx *wire.MsgTx, indexes []uint32) error {
	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("broadcast_txid", tx.TxHash()),
		logger.Uint32s("indexes", indexes),
	}, "No operation broadcaster received tx")
	return nil
}
