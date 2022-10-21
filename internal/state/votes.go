package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/tokenized/cacher"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
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

	Vote     *actions.Vote   `bsor:"3" json:"vote"`
	VoteTxID *bitcoin.Hash32 `bsor:"4" json:"vote_txid"`

	Result     *actions.Result `bsor:"5" json:"result"`
	ResultTxID *bitcoin.Hash32 `bsor:"6" json:"result_txid"`

	// ProposalScript is only used by Serialize to save the Proposal value in BSOR.
	ProposalScript bitcoin.Script `bsor:"8" json:"proposal_script"`

	// VoteScript is only used by Serialize to save the Vote value in BSOR.
	VoteScript bitcoin.Script `bsor:"9" json:"vote_script"`

	// ResultScript is only used by Serialize to save the Result value in BSOR.
	ResultScript bitcoin.Script `bsor:"10" json:"result_script"`

	// ContractWideVote bool `json:"ContractWideVote,omitempty"`

	// VoteTxId  *bitcoin.Hash32    `json:"VoteTxId,omitempty"`
	// TokenQty  uint64             `json:"TokenQty,omitempty"`
	// Expires   protocol.Timestamp `json:"Expires,omitempty"`
	// Timestamp protocol.Timestamp `json:"Timestamp,omitempty"`
	// CreatedAt protocol.Timestamp `json:"CreatedAt,omitempty"`
	// UpdatedAt protocol.Timestamp `json:"UpdatedAt,omitempty"`

	// OptionTally []uint64           `json:"OptionTally,omitempty"`
	// Result      string             `json:"Result,omitempty"`
	// AppliedTxId *bitcoin.Hash32    `json:"AppliedTxId,omitempty"`
	// CompletedAt protocol.Timestamp `json:"CompletedAt,omitempty"`

	// Ballots    map[bitcoin.Hash20]Ballot `json:"-"` // json can only encode string maps
	// BallotList []Ballot                  `json:"Ballots,omitempty"`

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

func (c *VoteCache) Add(ctx context.Context, vote *Vote) (*Vote, error) {
	item, err := c.cacher.Add(ctx, c.typ, vote)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	return item.(*Vote), nil
}

func (c *VoteCache) Get(ctx context.Context, voteTxID bitcoin.Hash32) (*Vote, error) {
	item, err := c.cacher.Get(ctx, c.typ, VotePath(voteTxID))
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*Vote), nil
}

func (c *VoteCache) Release(ctx context.Context, voteTxID bitcoin.Hash32) {
	c.cacher.Release(ctx, VotePath(voteTxID))
}

func VotePath(voteTxID bitcoin.Hash32) string {
	return fmt.Sprintf("%s/%s", votePath, voteTxID)
}

func (v *Vote) Path() string {
	return VotePath(*v.VoteTxID)
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
