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

	"github.com/pkg/errors"
)

const (
	contractVersion = uint8(0)
	contractPath    = "contracts"
)

type Contract struct {
	LockingScript bitcoin.Script `bsor:"1" json:"locking_script"`
	FormationTxID bitcoin.Hash32 `bsor:"2" json:"formation_txid"`
	KeyHash       bitcoin.Hash32 `bsor:"3" json:"key_hash"`

	sync.Mutex `bsor:"-"`
}

func NewContractCache(store storage.StreamStorage, fetcherCount, expireCount int,
	timeout time.Duration) (*cacher.Cache, error) {

	return cacher.NewCache(store, reflect.TypeOf(&Contract{}), contractPath, fetcherCount,
		expireCount, timeout)
}

func AddContract(ctx context.Context, cache *cacher.Cache, c *Contract) (*Contract, error) {
	item, err := cache.Add(ctx, c)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	return item.(*Contract), nil
}

func GetContract(ctx context.Context, cache *cacher.Cache, id bitcoin.Hash32) (*Contract, error) {
	item, err := cache.Get(ctx, id)
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*Contract), nil
}

func (c *Contract) ID() bitcoin.Hash32 {
	c.Lock()
	defer c.Unlock()

	return bitcoin.Hash32(sha256.Sum256(c.LockingScript))
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
