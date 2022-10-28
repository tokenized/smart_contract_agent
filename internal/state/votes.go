package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/tokenized/cacher"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

const (
	voteVersion = uint8(0)
	votePath    = "votes"
)

type VoteCache struct {
	cacher *cacher.Cache
	typ    reflect.Type
}

type Vote struct {
	Proposal     *actions.Proposal `bsor:"-" json:"proposal"`
	ProposalTxID *bitcoin.Hash32   `bsor:"2" json:"proposal_txid"`

	Vote     *actions.Vote   `bsor:"-" json:"vote"`
	VoteTxID *bitcoin.Hash32 `bsor:"4" json:"vote_txid"`

	Result     *actions.Result `bsor:"-" json:"result"`
	ResultTxID *bitcoin.Hash32 `bsor:"6" json:"result_txid"`

	// ProposalScript is only used by Serialize to save the Proposal value in BSOR.
	ProposalScript bitcoin.Script `bsor:"7" json:"proposal_script"`

	// VoteScript is only used by Serialize to save the Vote value in BSOR.
	VoteScript bitcoin.Script `bsor:"8" json:"vote_script"`

	// ResultScript is only used by Serialize to save the Result value in BSOR.
	ResultScript bitcoin.Script `bsor:"9" json:"result_script"`

	ContractWideVote bool   `bsor:"10" json:"contract_wide_vote"`
	TokenQuantity    uint64 `bsor:"11" json:"token_quantity"`

	isModified bool
	sync.Mutex `bsor:"-"`
}

func NewVoteCache(cache *cacher.Cache) (*VoteCache, error) {
	typ := reflect.TypeOf(&Vote{})

	// Verify item value type is valid for a cache item.
	if typ.Kind() != reflect.Ptr {
		return nil, errors.New("Type must be a pointer")
	}

	itemValue := reflect.New(typ.Elem())
	if !itemValue.CanInterface() {
		return nil, errors.New("Type must support interface")
	}

	itemInterface := itemValue.Interface()
	if _, ok := itemInterface.(cacher.CacheValue); !ok {
		return nil, errors.New("Type must implement CacheValue")
	}

	return &VoteCache{
		cacher: cache,
		typ:    typ,
	}, nil
}

func (c *VoteCache) ListActive(ctx context.Context, store storage.List,
	contractLockingScript bitcoin.Script) ([]*Vote, error) {

	contractHash := CalculateContractHash(contractLockingScript)
	pathPrefix := fmt.Sprintf("%s/%s", votePath, contractHash)

	paths, err := store.List(ctx, pathPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "list")
	}

	var result []*Vote
	for _, path := range paths {
		item, err := c.cacher.Get(ctx, c.typ, path)
		if err != nil {
			for _, v := range result {
				v.Lock()
				voteTxID := *v.VoteTxID
				v.Unlock()
				c.Release(ctx, contractLockingScript, voteTxID)
			}
			return nil, errors.Wrap(err, "get")
		}

		if item == nil {
			for _, v := range result {
				v.Lock()
				voteTxID := *v.VoteTxID
				v.Unlock()
				c.Release(ctx, contractLockingScript, voteTxID)
			}
			return nil, fmt.Errorf("Not found: %s", path)
		}

		vote := item.(*Vote)
		vote.Lock()
		isActive := vote.Result == nil
		vote.Unlock()

		if !isActive {
			c.cacher.Release(ctx, path)
		} else {
			result = append(result, vote)
		}
	}

	return result, nil
}

func (c *VoteCache) Add(ctx context.Context, contractLockingScript bitcoin.Script,
	vote *Vote) (*Vote, error) {

	vote.Lock()
	voteTxID := *vote.VoteTxID
	vote.Unlock()

	path := VotePath(CalculateContractHash(contractLockingScript), voteTxID)

	item, err := c.cacher.Add(ctx, c.typ, path, vote)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	return item.(*Vote), nil
}

func (c *VoteCache) Get(ctx context.Context, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32) (*Vote, error) {

	contractHash := CalculateContractHash(contractLockingScript)

	item, err := c.cacher.Get(ctx, c.typ, VotePath(contractHash, voteTxID))
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*Vote), nil
}

func (c *VoteCache) Release(ctx context.Context, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32) {
	c.cacher.Release(ctx, VotePath(CalculateContractHash(contractLockingScript), voteTxID))
}

func (v *Vote) Prepare(ctx context.Context, caches *Caches, contract *Contract,
	votingSystem *actions.VotingSystemField) error {

	contract.Lock()
	contractLockingScript := contract.LockingScript
	contract.Unlock()

	if len(v.Proposal.InstrumentCode) > 0 {
		instrumentHash20, err := bitcoin.NewHash20(v.Proposal.InstrumentCode)
		if err != nil {
			logger.Error(ctx, "Invalid proposal instrument code : %s", err)
			return nil
		}
		instrumentCode := InstrumentCode(*instrumentHash20)

		instrument, err := caches.Instruments.Get(ctx, contractLockingScript, instrumentCode)
		if err != nil {
			return errors.Wrap(err, "get instrument")
		}

		if instrument == nil {
			logger.Error(ctx, "Proposal instrument not found")
			return nil
		}
		defer caches.Instruments.Release(ctx, contractLockingScript, instrumentCode)

		instrument.Lock()
		v.ContractWideVote = instrument.Creation.InstrumentModificationGovernance == 1
		instrument.Unlock()
		v.MarkModified()
	}

	if len(v.Proposal.InstrumentCode) == 0 || v.ContractWideVote {
		if err := v.BuildContractBallots(ctx, caches, contract, votingSystem); err != nil {
			return errors.Wrap(err, "build contract ballots")
		}
	} else {
		if err := v.BuildInstrumentBallots(ctx, caches, contract, votingSystem); err != nil {
			return errors.Wrap(err, "instrument ballots")
		}
	}

	return nil
}

func (v *Vote) BuildInstrumentBallots(ctx context.Context, caches *Caches, contract *Contract,
	votingSystem *actions.VotingSystemField) error {

	// instrumentHash20, err := bitcoin.NewHash20(v.Proposal.InstrumentCode)
	// if err != nil {
	// 	logger.Error(ctx, "Invalid proposal instrument code : %s", err)
	// 	return nil
	// }
	// instrumentCode := InstrumentCode(*instrumentHash20)

	return nil
}

func (v *Vote) BuildContractBallots(ctx context.Context, caches *Caches, contract *Contract,
	votingSystem *actions.VotingSystemField) error {

	contract.Lock()
	contractLockingScript := contract.LockingScript
	instrumentCount := contract.InstrumentCount
	adminMemberInstrument := contract.AdminMemberInstrumentCode
	contract.Unlock()

	contractAddress, err := bitcoin.RawAddressFromLockingScript(contractLockingScript)
	if err != nil {
		return errors.Wrap(err, "contract address")
	}

	tokenQuantity := uint64(0)
	for i := uint64(0); i < instrumentCount; i++ {
		instrumentCode := InstrumentCode(protocol.InstrumentCodeFromContract(contractAddress, i))

		if v.Proposal.Type == 1 && instrumentCode.Equal(adminMemberInstrument) {
			continue // Administrative tokens don't count for holder votes.
		}

		instrument, err := caches.Instruments.Get(ctx, contractLockingScript, instrumentCode)
		if err != nil {
			return errors.Wrap(err, "get instrument")
		}

		if instrument == nil {
			continue
		}

		instrument.Lock()
		if instrument.Creation == nil {
			instrument.Unlock()
			caches.Instruments.Release(ctx, contractLockingScript, instrumentCode)
			continue
		}

		if !instrument.Creation.VotingRights {
			instrument.Unlock()
			caches.Instruments.Release(ctx, contractLockingScript, instrumentCode)
			continue
		}

		quantity := instrument.Creation.AuthorizedTokenQty
		multiplier := uint64(instrument.Creation.VoteMultiplier)
		instrument.Unlock()
		caches.Instruments.Release(ctx, contractLockingScript, instrumentCode)

		if votingSystem.VoteMultiplierPermitted {
			tokenQuantity += quantity * multiplier
		} else {
			tokenQuantity += quantity
		}

		ballots := make(map[bitcoin.Hash32]*Ballot)
		balances, err := caches.Balances.List(ctx, contractLockingScript, instrumentCode)
		if err != nil {
			return errors.Wrap(err, "list balances")
		}

		for _, balance := range balances {
			balance.Lock()
			hash := LockingScriptHash(balance.LockingScript)
			balanceQuantity := balance.Quantity
			balance.Unlock()

			if votingSystem.VoteMultiplierPermitted {
				balanceQuantity *= multiplier
			}

			ballot, exists := ballots[hash]
			if exists {
				ballot.Quantity += balanceQuantity
			} else {
				ballots[hash] = &Ballot{
					LockingScript: balance.LockingScript,
					Quantity:      balanceQuantity,
				}
			}
		}

		addedBallots, err := caches.Ballots.AddMulti(ctx, contractLockingScript, *v.VoteTxID,
			ballots)
		if err != nil {
			caches.Balances.ReleaseMulti(ctx, contractLockingScript, instrumentCode, balances)
			return errors.Wrap(err, "add ballots")
		}

		for hash, ballot := range ballots {
			addedBallot, exists := addedBallots[hash]
			if !exists {
				ballot.Lock()
				logger.ErrorWithFields(ctx, []logger.Field{
					logger.Stringer("locking_script", ballot.LockingScript),
				}, "Ballot failed to add")
				ballot.Unlock()
				continue
			}

			if addedBallot != ballot {
				// Pre-existing ballot, so update it.
				addedBallot.Quantity += ballot.Quantity
				addedBallot.MarkModified()
			}
		}

		caches.Ballots.ReleaseMulti(ctx, contractLockingScript, *v.VoteTxID, addedBallots)
		caches.Balances.ReleaseMulti(ctx, contractLockingScript, instrumentCode, balances)
	}

	v.TokenQuantity = tokenQuantity
	v.MarkModified()
	return nil
}

func VotePath(contractHash ContractHash, voteTxID bitcoin.Hash32) string {
	return fmt.Sprintf("%s/%s/%s", votePath, contractHash, voteTxID)
}

func (v *Vote) MarkModified() {
	v.isModified = true
}

func (v *Vote) ClearModified() {
	v.isModified = false
}

func (v *Vote) IsModified() bool {
	return v.isModified
}

func (v *Vote) CacheCopy() cacher.CacheValue {
	result := &Vote{
		ContractWideVote: v.ContractWideVote,
		TokenQuantity:    v.TokenQuantity,
	}

	isTest := IsTest()

	if v.Proposal != nil {
		copyScript, _ := protocol.Serialize(v.Proposal, isTest)
		action, _ := protocol.Deserialize(copyScript, isTest)
		result.Proposal, _ = action.(*actions.Proposal)
	}

	if v.ProposalTxID != nil {
		result.ProposalTxID = &bitcoin.Hash32{}
		copy(result.ProposalTxID[:], v.ProposalTxID[:])
	}

	if v.Vote != nil {
		copyScript, _ := protocol.Serialize(v.Vote, isTest)
		action, _ := protocol.Deserialize(copyScript, isTest)
		result.Vote, _ = action.(*actions.Vote)
	}

	if v.VoteTxID != nil {
		result.VoteTxID = &bitcoin.Hash32{}
		copy(result.VoteTxID[:], v.VoteTxID[:])
	}

	if v.Result != nil {
		copyScript, _ := protocol.Serialize(v.Result, isTest)
		action, _ := protocol.Deserialize(copyScript, isTest)
		result.Result, _ = action.(*actions.Result)
	}

	if v.ResultTxID != nil {
		result.ResultTxID = &bitcoin.Hash32{}
		copy(result.ResultTxID[:], v.ResultTxID[:])
	}

	return result
}

func (v *Vote) Serialize(w io.Writer) error {
	if v.Proposal != nil {
		script, err := protocol.Serialize(v.Proposal, IsTest())
		if err != nil {
			return errors.Wrap(err, "serialize proposal")
		}

		v.ProposalScript = script
	}

	if v.Vote != nil {
		script, err := protocol.Serialize(v.Vote, IsTest())
		if err != nil {
			return errors.Wrap(err, "serialize vote")
		}

		v.VoteScript = script
	}

	if v.Result != nil {
		script, err := protocol.Serialize(v.Result, IsTest())
		if err != nil {
			return errors.Wrap(err, "serialize result")
		}

		v.ResultScript = script
	}

	b, err := bsor.MarshalBinary(v)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := binary.Write(w, endian, voteVersion); err != nil {
		return errors.Wrap(err, "version")
	}

	if err := binary.Write(w, endian, uint32(len(b))); err != nil {
		return errors.Wrap(err, "size")
	}

	if _, err := w.Write(b); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

func (v *Vote) Deserialize(r io.Reader) error {
	var version uint8
	if err := binary.Read(r, endian, &version); err != nil {
		return errors.Wrap(err, "version")
	}

	if version != 0 {
		return fmt.Errorf("Unsupported version : %d", version)
	}

	var size uint32
	if err := binary.Read(r, endian, &size); err != nil {
		return errors.Wrap(err, "size")
	}

	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return errors.Wrap(err, "read")
	}

	v.Lock()
	defer v.Unlock()
	if _, err := bsor.UnmarshalBinary(b, v); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	if len(v.ProposalScript) != 0 {
		action, err := protocol.Deserialize(v.ProposalScript, IsTest())
		if err != nil {
			return errors.Wrap(err, "deserialize proposal")
		}

		proposal, ok := action.(*actions.Proposal)
		if !ok {
			return errors.New("ProposalScript is wrong type")
		}

		v.Proposal = proposal
	}

	if len(v.VoteScript) != 0 {
		action, err := protocol.Deserialize(v.VoteScript, IsTest())
		if err != nil {
			return errors.Wrap(err, "deserialize vote")
		}

		vote, ok := action.(*actions.Vote)
		if !ok {
			return errors.New("Vote is wrong type")
		}

		v.Vote = vote
	}

	if len(v.ResultScript) != 0 {
		action, err := protocol.Deserialize(v.ResultScript, IsTest())
		if err != nil {
			return errors.Wrap(err, "deserialize result")
		}

		result, ok := action.(*actions.Result)
		if !ok {
			return errors.New("Result is wrong type")
		}

		v.Result = result
	}

	return nil
}
