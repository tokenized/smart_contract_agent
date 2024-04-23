package platform

import (
	"fmt"

	"github.com/tokenized/pkg/bitcoin"
)

type DependsOnTxError struct {
	TxID  bitcoin.Hash32
	Index uint32
}

func NewDependsOnTxError(txid bitcoin.Hash32, index uint32) DependsOnTxError {
	return DependsOnTxError{
		TxID:  txid.Copy(),
		Index: index,
	}
}

func (e DependsOnTxError) Error() string {
	return fmt.Sprintf("Depends On: %s:%d", e.TxID, e.Index)
}
