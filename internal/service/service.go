package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	"github.com/tokenized/smart_contract_agent/pkg/contract_services"
	"github.com/tokenized/smart_contract_agent/pkg/locker"
	"github.com/tokenized/smart_contract_agent/pkg/scheduler"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	spynode "github.com/tokenized/spynode/pkg/client"

	"github.com/pkg/errors"
)

const (
	nextMessageIDPath = "next_message_id"
)

// Service feeds spynode blockchain data through an agent and posts the responses.
type Service struct {
	agentData agents.AgentData

	agent *agents.Agent

	config agents.Config

	spyNodeClient spynode.Client
	caches        *state.Caches
	transactions  *transactions.TransactionCache
	services      *contract_services.ContractServicesCache
	locker        locker.Locker
	store         storage.StreamStorage

	broadcaster          agents.Broadcaster
	fetcher              agents.Fetcher
	headers              agents.BlockHeaders
	scheduler            *scheduler.Scheduler
	peerChannelsFactory  *peer_channels.Factory
	peerChannelResponses chan agents.PeerChannelResponse

	nextSpyNodeMessageID uint64

	lock sync.Mutex
}

func NewService(agentData agents.AgentData, config agents.Config, spyNodeClient spynode.Client,
	caches *state.Caches, transactions *transactions.TransactionCache,
	services *contract_services.ContractServicesCache, locker locker.Locker,
	store storage.StreamStorage, broadcaster agents.Broadcaster, fetcher agents.Fetcher,
	headers agents.BlockHeaders, scheduler *scheduler.Scheduler,
	peerChannelsFactory *peer_channels.Factory,
	peerChannelResponses chan agents.PeerChannelResponse) *Service {

	return &Service{
		agentData:            agentData,
		config:               config,
		spyNodeClient:        spyNodeClient,
		caches:               caches,
		transactions:         transactions,
		services:             services,
		locker:               locker,
		store:                store,
		broadcaster:          broadcaster,
		fetcher:              fetcher,
		headers:              headers,
		scheduler:            scheduler,
		peerChannelsFactory:  peerChannelsFactory,
		peerChannelResponses: peerChannelResponses,
		nextSpyNodeMessageID: 1,
	}
}

func (s *Service) Load(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if b, err := s.store.Read(ctx, nextMessageIDPath); err != nil {
		if errors.Cause(err) != storage.ErrNotFound {
			return errors.Wrap(err, "read next message id")
		} else {
			s.nextSpyNodeMessageID = 1
		}
	} else {
		if err := binary.Read(bytes.NewReader(b), binary.LittleEndian,
			&s.nextSpyNodeMessageID); err != nil {
			return errors.Wrap(err, "next message id")
		}
	}

	agent, err := agents.NewAgent(ctx, s.agentData, s.config, s.caches, s.transactions, s.services,
		s.locker, s.store, s.broadcaster, s.fetcher, s.headers, s.scheduler, s,
		s.peerChannelsFactory, s.peerChannelResponses)
	if err != nil {
		return errors.Wrap(err, "new agent")
	}

	s.agent = agent

	if err := s.LoadEvents(ctx); err != nil {
		return errors.Wrap(err, "events")
	}

	return nil
}

func (s *Service) Save(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	w := &bytes.Buffer{}
	if err := binary.Write(w, binary.LittleEndian, s.nextSpyNodeMessageID); err != nil {
		return errors.Wrap(err, "next message id")
	}

	if err := s.store.Write(ctx, nextMessageIDPath, w.Bytes(), nil); err != nil {
		return errors.Wrap(err, "write")
	}

	if err := s.SaveEvents(ctx); err != nil {
		return errors.Wrap(err, "events")
	}

	return nil
}

func (s *Service) NextSpyNodeMessageID() uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.nextSpyNodeMessageID
}

func (s *Service) UpdateNextSpyNodeMessageID(id uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.nextSpyNodeMessageID = id + 1
}

func (s *Service) GetAgent(ctx context.Context,
	lockingScript bitcoin.Script) (*agents.Agent, error) {

	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.agentData.LockingScript.Equal(lockingScript) {
		return nil, nil
	}

	return s.agent.Copy(ctx), nil
}

func (s *Service) Release(ctx context.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.agent != nil {
		s.agent.Release(ctx)
		s.agent = nil
	}
}

func (s *Service) addTx(ctx context.Context, txid bitcoin.Hash32,
	spyNodeTx *spynode.Tx) (*transactions.Transaction, error) {

	transaction := &transactions.Transaction{
		Tx: spyNodeTx.Tx,
	}

	transaction.SpentOutputs = make([]*expanded_tx.Output, len(spyNodeTx.Outputs))
	for i, txout := range spyNodeTx.Outputs {
		transaction.SpentOutputs[i] = &expanded_tx.Output{
			Value:         txout.Value,
			LockingScript: txout.LockingScript,
		}
	}

	if spyNodeTx.State.Safe {
		transaction.State = transactions.TxStateSafe
	} else {
		if spyNodeTx.State.UnSafe {
			transaction.State |= transactions.TxStateUnsafe
		}
		if spyNodeTx.State.Cancelled {
			transaction.State |= transactions.TxStateCancelled
		}
	}

	if spyNodeTx.State.MerkleProof != nil {
		mp := spyNodeTx.State.MerkleProof.ConvertToMerkleProof(txid)
		transaction.MerkleProofs = []*merkle_proof.MerkleProof{mp}
	}

	addedTx, err := s.transactions.Add(ctx, transaction)
	if err != nil {
		return nil, errors.Wrap(err, "add tx")
	}

	if addedTx != transaction && spyNodeTx.State.MerkleProof != nil {
		mp := spyNodeTx.State.MerkleProof.ConvertToMerkleProof(txid)
		// Transaction already existed, so try to add the merkle proof to it.
		addedTx.Lock()
		addedTx.AddMerkleProof(mp)
		addedTx.Unlock()
	}

	return addedTx, nil
}
