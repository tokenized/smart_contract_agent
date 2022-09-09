package state

import (
	"bytes"
	"sync"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"

	"github.com/pkg/errors"
)

type InstrumentCode bitcoin.Hash20

// Instrument represents the intruments created under a contract and are stored with the contract.
type Instrument struct {
	InstrumentType [3]byte                     `bsor:"1" json:"instrument_type"`
	InstrumentCode InstrumentCode              `bsor:"2" json:"instrument_id"`
	ContractID     ContractID                  `bsor:"3" json:"contract_id"`
	Creation       *actions.InstrumentCreation `bsor:"4" json:"creation`
	CreationTxID   *bitcoin.Hash32             `bsor:"5" json:"creation_txid"`
	instrument     instruments.Instrument      `bsor:"-" json:"instrument"`

	sync.Mutex `bsor:"-"`
}

func (i *Instrument) ClearInstrument() {
	i.instrument = nil
}

func (i *Instrument) GetInstrument() (instruments.Instrument, error) {
	if i.instrument != nil {
		return i.instrument, nil
	}

	// Decode payload
	inst, err := instruments.Deserialize([]byte(i.Creation.InstrumentType),
		i.Creation.InstrumentPayload)
	if err != nil {
		return nil, errors.Wrap(err, "deserialize")
	}

	i.instrument = inst
	return i.instrument, nil
}

func (id InstrumentCode) Equal(other InstrumentCode) bool {
	return bytes.Equal(id[:], other[:])
}

func (id InstrumentCode) Bytes() []byte {
	return id[:]
}

func (id InstrumentCode) String() string {
	return bitcoin.Hash20(id).String()
}

func (id InstrumentCode) MarshalText() ([]byte, error) {
	return []byte(id.String()), nil
}

func (id *InstrumentCode) UnmarshalText(text []byte) error {
	h, err := bitcoin.NewHash20FromStr(string(text))
	if err != nil {
		return err
	}

	*id = InstrumentCode(*h)
	return nil
}
