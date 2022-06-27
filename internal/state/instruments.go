package state

import (
	"sync"

	"github.com/tokenized/pkg/bitcoin"
)

// Instrument represents the intruments created under a contract and are stored with the contract.
type Instrument struct {
	InstrumentType [3]byte        `bsor:"1" json:"instrument_type"`
	InstrumentID   bitcoin.Hash20 `bsor:"2" json:"instrument_id"`
	ContractID     bitcoin.Hash32 `bsor:"3" json:"contract_id"`
	CreationTxID   bitcoin.Hash32 `bsor:"4" json:"creation_txid"`
	CreationScript bitcoin.Script `bsor:"5" json:"creation_script`

	sync.Mutex `bsor:"-"`
}
