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
	ContractHash   ContractHash                `bsor:"3" json:"contract_hash"`
	Creation       *actions.InstrumentCreation `bsor:"-" json:"creation`
	CreationTxID   *bitcoin.Hash32             `bsor:"5" json:"creation_txid"`

	// CreationScript is only used by Serialize to save the Creation value in BSOR.
	CreationScript bitcoin.Script `bsor:"6" json:"creation_script"`

	FrozenUntil uint64 `bsor:"7" json:"frozen_until,omitempty"`

	// instrument is used to cache the deserialized value of the payload in Creation.
	instrument instruments.Instrument `json:"instrument"`

	sync.Mutex `bsor:"-"`
}

func (i *Instrument) ClearInstrument() {
	i.Lock()
	defer i.Unlock()

	i.instrument = nil
}

func (i *Instrument) GetPayload() (instruments.Instrument, error) {
	if i.instrument != nil {
		return i.instrument, nil
	}

	// Decode payload
	payload, err := instruments.Deserialize(i.InstrumentType[:], i.Creation.InstrumentPayload)
	if err != nil {
		return nil, errors.Wrap(err, "deserialize")
	}

	i.instrument = payload
	return i.instrument, nil
}

func (i *Instrument) TransfersPermitted() bool {
	payload, err := i.GetPayload()
	if err != nil {
		return false
	}

	switch pl := payload.(type) {
	case *instruments.Currency:
		return true
	case *instruments.Membership:
		return pl.TransfersPermitted
	case *instruments.ShareCommon:
		return pl.TransfersPermitted
	case *instruments.Coupon:
		return pl.TransfersPermitted
	case *instruments.LoyaltyPoints:
		return pl.TransfersPermitted
	case *instruments.TicketAdmission:
		return pl.TransfersPermitted
	case *instruments.CasinoChip:
		return pl.TransfersPermitted
	case *instruments.BondFixedRate:
		return pl.TransfersPermitted
	}

	return true
}

func (i *Instrument) IsFrozen(now uint64) bool {
	return i.FrozenUntil >= now
}

func (i *Instrument) IsExpired(now uint64) bool {
	payload, err := i.GetPayload()
	if err != nil {
		return false
	}

	switch pl := payload.(type) {
	case *instruments.Membership:
		return pl.ExpirationTimestamp != 0 && pl.ExpirationTimestamp < now
	case *instruments.ShareCommon:
	case *instruments.CasinoChip:
		return pl.ExpirationTimestamp != 0 && pl.ExpirationTimestamp < now
	case *instruments.Coupon:
		return pl.ExpirationTimestamp != 0 && pl.ExpirationTimestamp < now
	case *instruments.LoyaltyPoints:
		return pl.ExpirationTimestamp != 0 && pl.ExpirationTimestamp < now
	case *instruments.TicketAdmission:
		return pl.EventEndTimestamp != 0 && pl.EventEndTimestamp < now
	}

	return false
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
