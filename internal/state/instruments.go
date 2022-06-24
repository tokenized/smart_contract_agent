package state

import (
	"context"
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
	instrumentVersion = uint8(0)
	instrumentPath    = "instruments"
)

type Instrument struct {
	InstrumentType [3]byte        `bsor:"1" json:"instrument_type"`
	InstrumentID   bitcoin.Hash20 `bsor:"2" json:"instrument_id"`
	ContractID     bitcoin.Hash32 `bsor:"3" json:"contract_id"`
	CreationTxID   bitcoin.Hash32 `bsor:"4" json:"creation_txid"`
	CreationScript bitcoin.Script `bsor:"5" json:"creation_script`

	sync.Mutex `bsor:"-"`
}

func NewInstrumentCache(store storage.StreamStorage, fetcherCount, expireCount int,
	timeout time.Duration) (*cacher.Cache, error) {

	return cacher.NewCache(store, reflect.TypeOf(&Instrument{}), instrumentPath, fetcherCount,
		expireCount, timeout)
}

func AddInstrument(ctx context.Context, cache *cacher.Cache, c *Instrument) (*Instrument, error) {
	item, err := cache.Add(ctx, c)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	return item.(*Instrument), nil
}

func GetInstrument(ctx context.Context, cache *cacher.Cache,
	id bitcoin.Hash32) (*Instrument, error) {

	item, err := cache.Get(ctx, id)
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*Instrument), nil
}

func (i *Instrument) ID() bitcoin.Hash32 {
	i.Lock()
	defer i.Unlock()

	var id bitcoin.Hash32
	copy(id[:], i.InstrumentType[:])
	copy(id[3:], i.InstrumentID[:])
	return id
}

func (i *Instrument) Serialize(w io.Writer) error {
	i.Lock()
	b, err := bsor.MarshalBinary(i)
	if err != nil {
		i.Unlock()
		return errors.Wrap(err, "marshal")
	}
	i.Unlock()

	if err := binary.Write(w, endian, instrumentVersion); err != nil {
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

func (i *Instrument) Deserialize(r io.Reader) error {
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

	i.Lock()
	defer i.Unlock()
	if _, err := bsor.UnmarshalBinary(b, i); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	return nil
}
