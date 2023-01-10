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
	"github.com/tokenized/smart_contract_agent/pkg/locker"
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
	Proposal     *actions.Proposal `bsor:"1" json:"proposal"`
	ProposalTxID *bitcoin.Hash32   `bsor:"2" json:"proposal_txid"`

	VotingSystem *actions.VotingSystemField `bsor:"3" json:"voting_system"`

	Vote     *actions.Vote   `bsor:"4" json:"vote"`
	VoteTxID *bitcoin.Hash32 `bsor:"5" json:"vote_txid"`

	Result     *actions.Result `bsor:"5" json:"result"`
	ResultTxID *bitcoin.Hash32 `bsor:"7" json:"result_txid"`

	ContractWideVote bool      `bsor:"8" json:"contract_wide_vote"`
	TokenQuantity    uint64    `bsor:"9" json:"token_quantity"`
	BallotCount      uint64    `bsor:"10" json:"ballot_count"`
	BallotsCounted   uint64    `bsor:"11" json:"ballots_counted"`
	OptionTally      []float64 `bsor:"12" json:"option_tally"`
	VotedQuantity    uint64    `bsor:"13" json:"voted_quantity"`

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
	if _, ok := itemInterface.(cacher.Value); !ok {
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
	pathPrefix := fmt.Sprintf("%s/%s", contractHash, votePath)

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

func (c *VoteCache) ReleaseMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	votes []*Vote) {

	if len(votes) == 0 {
		return
	}

	for _, vote := range votes {
		vote.Lock()
		voteTxID := *vote.VoteTxID
		vote.Unlock()
		c.cacher.Release(ctx, VotePath(CalculateContractHash(contractLockingScript), voteTxID))
	}
}

func (v *Vote) Prepare(ctx context.Context, caches *Caches, locker locker.Locker,
	contract *Contract, votingSystem *actions.VotingSystemField, now *uint64) error {

	contract.Lock()
	contractLockingScript := contract.LockingScript
	contract.Unlock()

	v.OptionTally = make([]float64, len(v.Proposal.VoteOptions))
	v.VotingSystem = votingSystem

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
		if err := v.BuildContractBallots(ctx, caches, locker, contract, votingSystem,
			now); err != nil {
			return errors.Wrap(err, "build contract ballots")
		}
	} else {
		if err := v.BuildInstrumentBallots(ctx, caches, locker, contract, votingSystem,
			now); err != nil {
			return errors.Wrap(err, "instrument ballots")
		}
	}

	return nil
}

func (v *Vote) ApplyVote(choices string, quantity uint64) error {
	voteOptions := make([]byte, len(v.Proposal.VoteOptions))
	copy(voteOptions, []byte(v.Proposal.VoteOptions))

	quantityFloat := float64(quantity)
	voteMaxFloat := float64(v.Proposal.VoteMax)
	voteMax := int(v.Proposal.VoteMax)
	for i, choice := range []byte(choices) {
		var score float64
		switch v.VotingSystem.TallyLogic {
		case 0: // Standard
			score = quantityFloat
		case 1: // Weighted
			score = quantityFloat * (float64(voteMax-i) / voteMaxFloat)
		default:
			return fmt.Errorf("Unsupported tally logic : %d", v.VotingSystem.TallyLogic)
		}

		for j, option := range voteOptions {
			if option == choice {
				v.OptionTally[j] += score
				break
			}
		}
	}

	v.BallotsCounted++
	v.VotedQuantity += quantity
	return nil
}

type VoteResultDisplay struct {
	Results map[string]float64
}

// CalculateResults calculates the result of a completed vote.
func (v *Vote) CalculateResults(ctx context.Context) ([]uint64, string) {
	voteOptions := make([]byte, len(v.Proposal.VoteOptions))
	copy(voteOptions, []byte(v.Proposal.VoteOptions))

	var result []byte
	var highestIndex int
	var highestScore float64
	resultDisplay := make(map[string]float64)
	scored := make(map[int]bool)
	for {
		highestIndex = -1
		highestScore = 0.0
		for i, tally := range v.OptionTally {
			if _, exists := scored[i]; exists {
				continue
			}

			if tally <= highestScore {
				continue
			}

			switch v.VotingSystem.VoteType {
			case "R": // Relative
				if tally/float64(v.VotedQuantity) >= float64(v.VotingSystem.ThresholdPercentage)/100.0 {
					highestIndex = i
					highestScore = tally
				}
			case "A": // Absolute
				if tally/float64(v.TokenQuantity) >= float64(v.VotingSystem.ThresholdPercentage)/100.0 {
					highestIndex = i
					highestScore = tally
				}
			case "P": // Plurality
				highestIndex = i
				highestScore = tally
			}
		}

		if highestIndex == -1 {
			break // No more valid tallys
		}
		result = append(result, voteOptions[highestIndex])
		resultDisplay[string(voteOptions[highestIndex])] = highestScore
		scored[highestIndex] = true
	}

	// Convert tallys back to integers
	tallys := make([]uint64, len(v.OptionTally))
	for i, tally := range v.OptionTally {
		logger.Info(ctx, "Vote result %c : %f", voteOptions[i], tally)
		tallys[i] = uint64(tally)
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.JSON("results", resultDisplay),
	}, "Vote results calculated")
	return tallys, string(result)
}

func (v *Vote) BuildInstrumentBallots(ctx context.Context, caches *Caches, locker locker.Locker,
	contract *Contract, votingSystem *actions.VotingSystemField, now *uint64) error {

	instrumentHash20, err := bitcoin.NewHash20(v.Proposal.InstrumentCode)
	if err != nil {
		logger.Error(ctx, "Invalid proposal instrument code : %s", err)
		return nil
	}
	instrumentCode := InstrumentCode(*instrumentHash20)

	contract.Lock()
	contractLockingScript := contract.LockingScript
	adminAddress, err := bitcoin.DecodeRawAddress(contract.Formation.AdminAddress)
	if err != nil {
		contract.Unlock()
		return errors.Wrap(err, "admin address")
	}
	contract.Unlock()

	adminLockingScript, err := adminAddress.LockingScript()
	if err != nil {
		return errors.Wrap(err, "admin locking script")
	}

	instrument, err := caches.Instruments.Get(ctx, contractLockingScript, instrumentCode)
	if err != nil {
		return errors.Wrap(err, "get instrument")
	}

	if instrument == nil {
		return errors.New("Instrument not found")
	}

	instrument.Lock()
	if instrument.Creation == nil {
		instrument.Unlock()
		caches.Instruments.Release(ctx, contractLockingScript, instrumentCode)
		return errors.New("Instrument creation missing")
	}

	if !instrument.Creation.VotingRights {
		instrument.Unlock()
		caches.Instruments.Release(ctx, contractLockingScript, instrumentCode)
		return nil
	}

	multiplier := uint64(instrument.Creation.VoteMultiplier)
	instrument.Unlock()
	caches.Instruments.Release(ctx, contractLockingScript, instrumentCode)

	tokenQuantity := uint64(0)
	ballotCount := uint64(0)
	ballots := make(map[bitcoin.Hash32]*Ballot)
	balances, err := caches.Balances.List(ctx, contractLockingScript, instrumentCode)
	if err != nil {
		return errors.Wrap(err, "list balances")
	}
	defer caches.Balances.ReleaseMulti(ctx, contractLockingScript, instrumentCode, balances)

	lockerResponseChannel := locker.AddRequest(BalanceSet{balances})
	lockerResponse := <-lockerResponseChannel
	switch v := lockerResponse.(type) {
	case uint64:
		*now = v
	case error:
		return errors.Wrap(v, "locker")
	}

	for _, balance := range balances {
		if adminLockingScript.Equal(balance.LockingScript) {
			continue
		}
		hash := LockingScriptHash(balance.LockingScript)
		balanceQuantity := balance.Quantity

		tokenQuantity += balanceQuantity
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

	balances.Unlock()

	if votingSystem.VoteMultiplierPermitted {
		tokenQuantity *= multiplier
	}

	addedBallots, err := caches.Ballots.AddMulti(ctx, contractLockingScript, *v.VoteTxID,
		ballots)
	if err != nil {
		return errors.Wrap(err, "add ballots")
	}
	defer caches.Ballots.ReleaseMulti(ctx, contractLockingScript, *v.VoteTxID, addedBallots)

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
		} else {
			ballotCount++
		}
	}

	v.TokenQuantity = tokenQuantity
	v.BallotCount = ballotCount
	v.MarkModified()
	return nil
}

func (v *Vote) BuildContractBallots(ctx context.Context, caches *Caches, locker locker.Locker,
	contract *Contract, votingSystem *actions.VotingSystemField, now *uint64) error {

	contract.Lock()
	if contract.Formation == nil {
		contract.Unlock()
		return errors.New("Missing contract formation")
	}
	contractLockingScript := contract.LockingScript
	adminAddress, err := bitcoin.DecodeRawAddress(contract.Formation.AdminAddress)
	if err != nil {
		contract.Unlock()
		return errors.Wrap(err, "admin address")
	}
	instrumentCount := contract.InstrumentCount
	adminMemberInstrument := contract.AdminMemberInstrumentCode
	contract.Unlock()

	contractAddress, err := bitcoin.RawAddressFromLockingScript(contractLockingScript)
	if err != nil {
		return errors.Wrap(err, "contract address")
	}

	adminLockingScript, err := adminAddress.LockingScript()
	if err != nil {
		return errors.Wrap(err, "admin locking script")
	}

	totalTokenQuantity := uint64(0)
	totalBallotCount := uint64(0)
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

		tokenQuantity := uint64(0)
		multiplier := uint64(instrument.Creation.VoteMultiplier)
		instrument.Unlock()
		caches.Instruments.Release(ctx, contractLockingScript, instrumentCode)

		ballots := make(map[bitcoin.Hash32]*Ballot)
		balances, err := caches.Balances.List(ctx, contractLockingScript, instrumentCode)
		if err != nil {
			return errors.Wrap(err, "list balances")
		}
		defer caches.Balances.ReleaseMulti(ctx, contractLockingScript, instrumentCode, balances)

		lockerResponseChannel := locker.AddRequest(BalanceSet{balances})
		lockerResponse := <-lockerResponseChannel
		switch v := lockerResponse.(type) {
		case uint64:
			*now = v
		case error:
			return errors.Wrap(v, "locker")
		}

		for _, balance := range balances {
			if adminLockingScript.Equal(balance.LockingScript) {
				continue
			}
			hash := LockingScriptHash(balance.LockingScript)
			balanceQuantity := balance.Quantity

			tokenQuantity += balanceQuantity
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

		balances.Unlock()

		if votingSystem.VoteMultiplierPermitted {
			tokenQuantity *= multiplier
		}
		totalTokenQuantity += tokenQuantity

		addedBallots, err := caches.Ballots.AddMulti(ctx, contractLockingScript, *v.VoteTxID,
			ballots)
		if err != nil {
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
			} else {
				totalBallotCount++
			}
		}

		caches.Ballots.ReleaseMulti(ctx, contractLockingScript, *v.VoteTxID, addedBallots)
	}

	v.TokenQuantity = totalTokenQuantity
	v.BallotCount = totalBallotCount
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

func (v *Vote) CacheCopy() cacher.Value {
	result := &Vote{
		ContractWideVote: v.ContractWideVote,
		TokenQuantity:    v.TokenQuantity,
	}

	if v.Proposal != nil {
		result.Proposal = v.Proposal.Copy()
	}

	if v.ProposalTxID != nil {
		result.ProposalTxID = &bitcoin.Hash32{}
		copy(result.ProposalTxID[:], v.ProposalTxID[:])
	}

	if v.Vote != nil {
		result.Vote = v.Vote.Copy()
	}

	if v.VoteTxID != nil {
		result.VoteTxID = &bitcoin.Hash32{}
		copy(result.VoteTxID[:], v.VoteTxID[:])
	}

	if v.Result != nil {
		result.Result = v.Result.Copy()
	}

	if v.ResultTxID != nil {
		result.ResultTxID = &bitcoin.Hash32{}
		copy(result.ResultTxID[:], v.ResultTxID[:])
	}

	if v.VotingSystem != nil {
		result.VotingSystem = v.VotingSystem.Copy()
	}

	return result
}

func (v *Vote) Serialize(w io.Writer) error {
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

	return nil
}
