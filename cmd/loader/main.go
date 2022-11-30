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
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/internal/whatsonchain"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
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

	WhatsOnChainAPIKey string          `envconfig:"WOC_API_KEY" json:"api_key" masked:"true"`
	Network            bitcoin.Network `default:"mainnet" json:"network" envconfig:"NETWORK"`

	Storage storage.Config     `json:"storage"`
	Cache   cacher.Config      `json:"cache"`
	Logger  logger.SetupConfig `json:"logger"`
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

	woc := whatsonchain.NewService(cfg.WhatsOnChainAPIKey, cfg.Network)

	store, err := storage.CreateStreamStorage(cfg.Storage.Bucket, cfg.Storage.Root,
		cfg.Storage.MaxRetries, cfg.Storage.RetryDelay)
	if err != nil {
		logger.Fatal(ctx, "Failed to create storage : %s", err)
	}

	cache := cacher.NewCache(store, cfg.Cache)
	caches, err := state.NewCaches(cache)
	if err != nil {
		logger.Fatal(ctx, "Failed to create caches : %s", err)
	}

	broadcaster := NewNoopBroadcaster()

	factory := NewFactory()

	peerChannelsFactory := peer_channels.NewFactory()

	lockingScript, err := cfg.AgentKey.LockingScript()
	if err != nil {
		logger.Fatal(ctx, "Failed to create agent locking script : %s", err)
	}

	agent, err := agents.NewAgent(ctx, cfg.AgentKey, lockingScript, cfg.Agents, feeLockingScript,
		caches, store, broadcaster, woc, woc, nil, factory, peerChannelsFactory)
	if err != nil {
		logger.Fatal(ctx, "Failed to create agent : %s", err)
	}

	factory.SetAgent(agent)

	var cacheWait, loadWait sync.WaitGroup

	cacheShutdown := make(chan error)
	cacheThread, cacheComplete := threads.NewInterruptableThreadComplete("Cache",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return cache.Run(ctx, interrupt, cacheShutdown)
		}, &cacheWait)

	loadThread, loadComplete := threads.NewInterruptableThreadComplete("Load",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return load(ctx, interrupt, agent, caches.Transactions, woc)
		}, &loadWait)

	cacheThread.Start(ctx)

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
		logger.Info(ctx, "Start shutdown...")
	}

	loadThread.Stop(ctx)
	loadWait.Wait()

	agent.Release(ctx)

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

func loadTx(ctx context.Context, agent *agents.Agent, transactions *state.TransactionCache,
	woc *whatsonchain.Service, txid bitcoin.Hash32) error {

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("txid", txid),
	}, "Loading transaction")

	transaction, err := getTx(ctx, transactions, woc, txid)
	if err != nil {
		return errors.Wrapf(err, "get transaction: %s", txid)
	}
	defer transactions.Release(ctx, txid)

	if err := getInputs(ctx, transactions, woc, transaction); err != nil {
		return errors.Wrapf(err, "get inputs: %s", txid)
	}

	if err := agent.UpdateTransaction(ctx, transaction, uint64(time.Now().UnixNano())); err != nil {
		return errors.Wrapf(err, "update transaction")
	}

	return nil
}

func load(ctx context.Context, interrupt <-chan interface{}, agent *agents.Agent,
	transactions *state.TransactionCache, woc *whatsonchain.Service) error {

	history, err := woc.GetLockingScriptHistory(ctx, agent.LockingScript())
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

		if err := loadTx(ctx, agent, transactions, woc, history[index].TxID); err != nil {
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

func (*NoopBroadcaster) BroadcastTx(ctx context.Context, etx *expanded_tx.ExpandedTx,
	indexes []uint32) error {

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("broadcast_txid", etx.Tx.TxHash()),
		logger.Uint32s("indexes", indexes),
	}, "No operation broadcaster received tx")
	return nil
}

type Factory struct {
	lockingScript bitcoin.Script
	agent         *agents.Agent

	lock sync.Mutex
}

func NewFactory() *Factory {
	return &Factory{}
}

func (f *Factory) SetAgent(agent *agents.Agent) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.lockingScript = agent.LockingScript()
	f.agent = agent
}

func (f *Factory) GetAgent(ctx context.Context,
	lockingScript bitcoin.Script) (*agents.Agent, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	if !f.lockingScript.Equal(lockingScript) {
		return nil, nil
	}

	return f.agent.Copy(ctx), nil
}
