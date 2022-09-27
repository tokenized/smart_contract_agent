package state

import (
	"bytes"
	"sync"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

// Instrument represents the intruments created under a contract and are stored with the contract.
type Instrument struct {
	InstrumentType [3]byte                     `bsor:"1" json:"instrument_type"`
	InstrumentCode InstrumentCode              `bsor:"2" json:"instrument_id"`
	ContractID     ContractID                  `bsor:"3" json:"contract_id"`
	Creation       *actions.InstrumentCreation `bsor:"-" json:"creation`
	CreationTxID   *bitcoin.Hash32             `bsor:"5" json:"creation_txid"`

	// CreationScript is only used by Serialize to save the Creation value in BSOR.
	CreationScript bitcoin.Script `bsor:"6" json:"creation_script"`

	// instrument is used to cache the deserialized value of the payload in Creation.
	instrument instruments.Instrument `json:"instrument"`

	sync.Mutex `bsor:"-"`
}

func (i *Instrument) ClearInstrument() {
	i.Lock()
	defer i.Unlock()

	i.instrument = nil
}

func (i *Instrument) GetInstrument() (instruments.Instrument, error) {
	i.Lock()
	defer i.Unlock()

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

func (i *Instrument) PrepareForMarshal() error {
	i.Lock()
	defer i.Unlock()

	if i.Creation != nil {
		script, err := protocol.Serialize(i.Creation, IsTest())
		if err != nil {
			return errors.Wrap(err, "serialize instrument creation")
		}

		i.CreationScript = script
	}

	return nil
}

func (i *Instrument) CompleteUnmarshal() error {
	i.Lock()
	defer i.Unlock()

	if len(i.CreationScript) != 0 {
		action, err := protocol.Deserialize(i.CreationScript, IsTest())
		if err != nil {
			return errors.Wrap(err, "deserialize instrument creation")
		}

		creation, ok := action.(*actions.InstrumentCreation)
		if !ok {
			return errors.New("CreationScript is wrong type")
		}

		i.Creation = creation
	}

	return nil
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
