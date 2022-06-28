package state

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/cacher"
	"github.com/tokenized/specification/dist/golang/actions"

	"github.com/pkg/errors"
)

const (
	contractVersion = uint8(0)
	contractPath    = "contracts"
)

type ContractCache struct {
	cacher *cacher.Cache
}

type Contract struct {
	KeyHash                      bitcoin.Hash32                    `bsor:"1" json:"key_hash"`
	LockingScript                bitcoin.Script                    `bsor:"2" json:"locking_script"`
	Formation                    *actions.ContractFormation        `bsor:"3" json:"formation"`
	FormationTxID                *bitcoin.Hash32                   `bsor:"4" json:"formation_txid"`
	BodyOfAgreementFormation     *actions.BodyOfAgreementFormation `bsor:"5" json:"body_of_agreement_formation"`
	BodyOfAgreementFormationTxID *bitcoin.Hash32                   `bsor:"6" json:"body_of_agreement_formation_txid"`

	Instruments []*Instrument `bsor:"7" json:"instruments"`

	sync.Mutex `bsor:"-"`
}

type ContractLookup struct {
	KeyHash       bitcoin.Hash32 `json:"key_hash"`
	LockingScript bitcoin.Script `json:"locking_script"`
}

type ContractID bitcoin.Hash32

func NewContractCache(store storage.StreamStorage, fetcherCount, expireCount int,
	expiration, fetchTimeout time.Duration) (*ContractCache, error) {

	cacher, err := cacher.NewCache(store, reflect.TypeOf(&Contract{}), fetcherCount, expireCount,
		expiration, fetchTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "cacher")
	}

	return &ContractCache{
		cacher: cacher,
	}, nil
}

func (c *ContractCache) List(ctx context.Context,
	store storage.StreamStorage) ([]*ContractLookup, error) {

	paths, err := store.List(ctx, contractPath)
	if err != nil {
		return nil, errors.Wrap(err, "list")
	}

	var result []*ContractLookup
	for _, path := range paths {
		item, err := c.cacher.Get(ctx, path)
		if err != nil {
			return nil, errors.Wrap(err, "get")
		}

		if item == nil {
			return nil, fmt.Errorf("Not found: %s", path)
		}

		contract := item.(*Contract)
		contract.Lock()
		result = append(result, &ContractLookup{
			KeyHash:       contract.KeyHash,
			LockingScript: contract.LockingScript,
		})
		contract.Unlock()

		c.cacher.Release(ctx, path)
	}

	return result, nil
}

func (c *ContractCache) Run(ctx context.Context, interrupt <-chan interface{}) error {
	return c.cacher.Run(ctx, interrupt)
}

func (c *ContractCache) Add(ctx context.Context, contract *Contract) (*Contract, error) {
	item, err := c.cacher.Add(ctx, contract)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	return item.(*Contract), nil
}

func (c *ContractCache) Get(ctx context.Context, lockingScript bitcoin.Script) (*Contract, error) {
	item, err := c.cacher.Get(ctx, ContractPath(lockingScript))
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*Contract), nil
}

func (c *ContractCache) Save(ctx context.Context, contract *Contract) error {
	return c.cacher.Save(ctx, contract)
}

func (c *ContractCache) Release(ctx context.Context, lockingScript bitcoin.Script) {
	c.cacher.Release(ctx, ContractPath(lockingScript))
}

func CalculateContractID(lockingScript bitcoin.Script) ContractID {
	return ContractID(sha256.Sum256(lockingScript))
}

func ContractPath(lockingScript bitcoin.Script) string {
	return fmt.Sprintf("%s/%s", contractPath, CalculateContractID(lockingScript))
}

func (c *Contract) Path() string {
	c.Lock()
	defer c.Unlock()

	return ContractPath(c.LockingScript)
}

func (c *Contract) Serialize(w io.Writer) error {
	c.Lock()
	b, err := bsor.MarshalBinary(c)
	if err != nil {
		c.Unlock()
		return errors.Wrap(err, "marshal")
	}
	c.Unlock()

	if err := binary.Write(w, endian, contractVersion); err != nil {
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

func (c *Contract) Deserialize(r io.Reader) error {
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

	c.Lock()
	defer c.Unlock()
	if _, err := bsor.UnmarshalBinary(b, c); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	return nil
}
