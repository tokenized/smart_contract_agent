package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/tokenized/config"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/whatsonchain"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	"github.com/tokenized/smart_contract_agent/pkg/contract_services"
	"github.com/tokenized/smart_contract_agent/pkg/locker"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

var (
	buildVersion = "unknown"
	buildDate    = "unknown"
	buildUser    = "unknown"
)

type Config struct {
	AgentData agents.AgentData `json:"agent_data"`
	Agents    agents.Config    `json:"agents"`

	WhatsOnChainAPIKey         string          `envconfig:"WOC_API_KEY" json:"api_key" masked:"true"`
	WhatsOnChainDialTimeout    config.Duration `default:"10s" envconfig:"WOC_DIAL_TIMEOUT" json:"woc_dial_timeout"`
	WhatsOnChainRequestTimeout config.Duration `default:"30s" envconfig:"WOC_REQUEST_TIMEOUT" json:"woc_request_timeout"`

	Network bitcoin.Network `default:"mainnet" json:"network" envconfig:"NETWORK"`

	ChannelTimeout config.Duration `default:"1s" envconfig:"CHANNEL_TIMEOUT" json:"channel_timeout"`

	Storage storage.Config     `json:"storage"`
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

	woc := whatsonchain.NewService(cfg.WhatsOnChainAPIKey, cfg.Network,
		cfg.WhatsOnChainDialTimeout.Duration, cfg.WhatsOnChainRequestTimeout.Duration)

	store, err := storage.CreateStreamStorage(cfg.Storage.Bucket, cfg.Storage.Root,
		cfg.Storage.MaxRetries, cfg.Storage.RetryDelay)
	if err != nil {
		logger.Fatal(ctx, "Failed to create storage : %s", err)
	}

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

	locker := locker.NewThreadedLocker(1000)

	broadcaster := NewNoopBroadcaster()

	agentStore := NewStore()

	peerChannelsFactory := peer_channels.NewFactory()

	peerChannelResponses := make(chan agents.PeerChannelResponse)

	dependencyTrigger := agents.NewDependencyTrigger(1, cfg.ChannelTimeout.Duration, transactions)

	agent, err := agents.NewAgent(ctx, cfg.AgentData, cfg.Agents, caches, transactions, services,
		locker, store, broadcaster, woc, woc, nil, agentStore, peerChannelsFactory,
		peerChannelResponses, nil, dependencyTrigger.Trigger)
	if err != nil {
		logger.Fatal(ctx, "Failed to create agent : %s", err)
	}

	agentStore.SetAgent(agent)

	var lockerWait, peerChannelResponseWait, loadWait, dependencyTriggerWait sync.WaitGroup

	lockerThread, lockerComplete := threads.NewInterruptableThreadComplete("Balance Locker",
		locker.Run, &lockerWait)

	peerChannelResponder := agents.NewPeerChannelResponder(caches, peerChannelsFactory)
	peerChannelResponseThread, peerChannelResponseComplete := threads.NewUninterruptableThreadComplete("Peer Channel Response",
		func(ctx context.Context) error {
			return agents.ProcessResponses(ctx, peerChannelResponder, peerChannelResponses)
		}, &peerChannelResponseWait)

	dependencyTriggerThread, dependencyTriggerComplete := threads.NewInterruptableThreadComplete("Dependency Trigger",
		dependencyTrigger.Run, &dependencyTriggerWait)

	loadThread, loadComplete := threads.NewInterruptableThreadComplete("Load",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return load(ctx, interrupt, agent, transactions, woc)
		}, &loadWait)

	lockerThread.Start(ctx)
	peerChannelResponseThread.Start(ctx)
	dependencyTriggerThread.Start(ctx)
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
	case err := <-lockerComplete:
		logger.Error(ctx, "Balance locker completed : %s", err)

	case err := <-peerChannelResponseComplete:
		logger.Error(ctx, "Peer Channel Response completed : %s", err)

	case err := <-dependencyTriggerComplete:
		logger.Error(ctx, "Dependency Trigger completed : %s", err)

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

	dependencyTriggerThread.Stop(ctx)
	dependencyTriggerWait.Wait()

	lockerThread.Stop(ctx)
	lockerWait.Wait()

	close(peerChannelResponses)
	peerChannelResponseWait.Wait()

	agent.Release(ctx)
}

func getTx(ctx context.Context, transactionsCache *transactions.TransactionCache,
	woc *whatsonchain.Service, txid bitcoin.Hash32) (*transactions.Transaction, error) {

	if transaction, err := transactionsCache.Get(ctx, txid); err != nil {
		return nil, errors.Wrapf(err, "get transaction: %s", txid)
	} else if transaction != nil {
		return transaction, nil
	}

	logger.Info(ctx, "Fetching transaction")
	gotTx, err := woc.GetTx(ctx, txid)
	if err != nil {
		return nil, errors.Wrapf(err, "woc get tx: %s", txid)
	}

	newTransaction := &transactions.Transaction{
		Tx:    gotTx,
		State: transactions.TxStateSafe, // assume safe because it is history
	}
	return transactionsCache.Add(ctx, newTransaction)
}

func getInputs(ctx context.Context, transactions *transactions.TransactionCache,
	woc *whatsonchain.Service, transaction *transactions.Transaction) error {

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

func loadTx(ctx context.Context, agent *agents.Agent, transactions *transactions.TransactionCache,
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

	isTest := agent.Config().IsTest
	transaction.Lock()
	actionList, err := agents.CompileActions(ctx, transaction, isTest)
	transaction.Unlock()
	if err != nil {
		return errors.Wrapf(err, "compile actions: %s", txid)
	}

	if err := agent.UpdateTransaction(ctx, transaction, actionList); err != nil {
		return errors.Wrapf(err, "update transaction: %s", txid)
	}

	return nil
}

func load(ctx context.Context, interrupt <-chan interface{}, agent *agents.Agent,
	transactions *transactions.TransactionCache, woc *whatsonchain.Service) error {

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

type Store struct {
	lockingScript bitcoin.Script
	agent         *agents.Agent

	lock sync.Mutex
}

func NewStore() *Store {
	return &Store{}
}

func (s *Store) SetAgent(agent *agents.Agent) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.lockingScript = agent.LockingScript()
	s.agent = agent
}

func (s *Store) GetAgent(ctx context.Context,
	lockingScript bitcoin.Script) (*agents.Agent, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.lockingScript.Equal(lockingScript) {
		return nil, nil
	}

	return s.agent.Copy(ctx), nil
}

func (s *Store) Release(ctx context.Context, agent *agents.Agent) {
	s.agent.Release(ctx)
}
