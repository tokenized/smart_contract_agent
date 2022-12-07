package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	spynode "github.com/tokenized/spynode/pkg/client"

	"github.com/pkg/errors"
)

const (
	nextMessageIDPath = "next_message_id"
)

// Service feeds spynode blockchain data through an agent and posts the responses.
type Service struct {
	key           bitcoin.Key
	lockingScript bitcoin.Script

	agent *agents.Agent

	config           agents.Config
	feeLockingScript bitcoin.Script

	spyNodeClient spynode.Client
	caches        *state.Caches
	balanceLocker state.BalanceLocker
	store         storage.StreamStorage

	broadcaster         agents.Broadcaster
	fetcher             agents.Fetcher
	headers             agents.BlockHeaders
	scheduler           *platform.Scheduler
	peerChannelsFactory *peer_channels.Factory

	nextSpyNodeMessageID uint64

	lock sync.Mutex
}

func NewService(key bitcoin.Key, lockingScript bitcoin.Script, config agents.Config,
	feeLockingScript bitcoin.Script, spyNodeClient spynode.Client, caches *state.Caches,
	balanceLocker state.BalanceLocker, store storage.StreamStorage, broadcaster agents.Broadcaster,
	fetcher agents.Fetcher, headers agents.BlockHeaders, scheduler *platform.Scheduler,
	peerChannelsFactory *peer_channels.Factory) *Service {

	return &Service{
		key:                  key,
		lockingScript:        lockingScript,
		config:               config,
		feeLockingScript:     feeLockingScript,
		spyNodeClient:        spyNodeClient,
		caches:               caches,
		balanceLocker:        balanceLocker,
		store:                store,
		broadcaster:          broadcaster,
		fetcher:              fetcher,
		headers:              headers,
		scheduler:            scheduler,
		peerChannelsFactory:  peerChannelsFactory,
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

	agent, err := agents.NewAgent(ctx, s.key, s.lockingScript, s.config, s.feeLockingScript,
		s.caches, s.balanceLocker, s.store, s.broadcaster, s.fetcher, s.headers, s.scheduler, s,
		s.peerChannelsFactory)
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

	if !s.lockingScript.Equal(lockingScript) {
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
