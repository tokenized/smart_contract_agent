package state

import (
	"sync"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
)

type InstrumentCode bitcoin.Hash20

// Instrument represents the intruments created under a contract and are stored with the contract.
type Instrument struct {
	InstrumentType [3]byte                     `bsor:"1" json:"instrument_type"`
	InstrumentCode InstrumentCode              `bsor:"2" json:"instrument_id"`
	ContractID     ContractID                  `bsor:"3" json:"contract_id"`
	Creation       *actions.InstrumentCreation `bsor:"4" json:"creation`
	CreationTxID   *bitcoin.Hash32             `bsor:"5" json:"creation_txid"`
	Instrument     instruments.Instrument      `bsor:"6" json:"instrument"`

	sync.Mutex `bsor:"-"`
}
