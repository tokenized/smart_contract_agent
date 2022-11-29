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

	"github.com/pkg/errors"
)

const (
	recoveryTxsVersion = uint8(0)
	recoveryTxsPath    = "recovery_txs"
)

type RecoveryTransactionsCache struct {
	cacher *cacher.Cache
	typ    reflect.Type
}

// RecoveryTransactions are request transactions received when in "recovery" mode, meaning that the
// agent doesn't know which transactions it has already responded to. So it waits until it sees all
// transactions, while collecting any requests, and removing any requests that it sees responses to,
// then responds to any requests that haven't been responded to.
type RecoveryTransactions struct {
	Transactions []*RecoveryTransaction `bsor:"1" json:"transactions"`

	isModified bool
	sync.Mutex `bsor:"-"`
}

type RecoveryTransaction struct {
	TxID          bitcoin.Hash32 `bsor:"1" json:"txid"`
	OutputIndexes []int          `bsor:"2" json:"output_indexes"`
}

func NewRecoveryTransactionsCache(cache *cacher.Cache) (*RecoveryTransactionsCache, error) {
	typ := reflect.TypeOf(&RecoveryTransactions{})

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

	return &RecoveryTransactionsCache{
		cacher: cache,
		typ:    typ,
	}, nil
}

func (c *RecoveryTransactionsCache) Add(ctx context.Context, lockingScript bitcoin.Script,
	recoveryTransactions *RecoveryTransactions) (*RecoveryTransactions, error) {

	item, err := c.cacher.Add(ctx, c.typ, RecoveryTxsPath(lockingScript), recoveryTransactions)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	return item.(*RecoveryTransactions), nil
}

func (c *RecoveryTransactionsCache) Get(ctx context.Context,
	lockingScript bitcoin.Script) (*RecoveryTransactions, error) {

	item, err := c.cacher.Get(ctx, c.typ, RecoveryTxsPath(lockingScript))
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*RecoveryTransactions), nil
}

func (c *RecoveryTransactionsCache) Release(ctx context.Context, lockingScript bitcoin.Script) {
	c.cacher.Release(ctx, RecoveryTxsPath(lockingScript))
}

func RecoveryTxsPath(lockingScript bitcoin.Script) string {
	return fmt.Sprintf("%s/%s", CalculateContractHash(lockingScript), recoveryTxsPath)
}

func (txs *RecoveryTransactions) Append(tx *RecoveryTransaction) {
	for _, t := range txs.Transactions {
		if t.TxID.Equal(&tx.TxID) {
			return // already have this tx
		}
	}

	txs.Transactions = append(txs.Transactions, tx)
	txs.isModified = true
}

func (txs *RecoveryTransactions) Remove(txid bitcoin.Hash32, outputIndex int) bool {
	// Remove from recovery requests.
	for i, tx := range txs.Transactions {
		if !tx.TxID.Equal(&txid) {
			continue
		}

		if tx.Remove(outputIndex) {
			if len(tx.OutputIndexes) == 0 {
				txs.Transactions = append(txs.Transactions[:i], txs.Transactions[i+1:]...)
			}

			txs.isModified = true
			return true
		} else {
			return false
		}
	}

	return false
}

func (tx *RecoveryTransaction) Remove(outputIndex int) bool {
	for i, oi := range tx.OutputIndexes {
		if oi == outputIndex {
			tx.OutputIndexes = append(tx.OutputIndexes[:i], tx.OutputIndexes[i+1:]...)
			return true
		}
	}

	return false
}

func (txs *RecoveryTransactions) MarkModified() {
	txs.isModified = true
}

func (txs *RecoveryTransactions) ClearModified() {
	txs.isModified = false
}

func (txs *RecoveryTransactions) IsModified() bool {
	return txs.isModified
}

func (txs *RecoveryTransactions) CacheCopy() cacher.CacheValue {
	result := &RecoveryTransactions{
		Transactions: make([]*RecoveryTransaction, len(txs.Transactions)),
	}

	for i, transaction := range txs.Transactions {
		cpy := transaction.Copy()
		result.Transactions[i] = &cpy
	}

	return result
}

func (tx RecoveryTransaction) Copy() RecoveryTransaction {
	result := RecoveryTransaction{
		OutputIndexes: make([]int, len(tx.OutputIndexes)),
	}

	copy(result.TxID[:], tx.TxID[:])
	for i, outputIndex := range tx.OutputIndexes {
		result.OutputIndexes[i] = outputIndex
	}

	return result
}

func (txs *RecoveryTransactions) Serialize(w io.Writer) error {
	b, err := bsor.MarshalBinary(txs)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := binary.Write(w, endian, recoveryTxsVersion); err != nil {
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

func (txs *RecoveryTransactions) Deserialize(r io.Reader) error {
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

	txs.Lock()
	defer txs.Unlock()
	if _, err := bsor.UnmarshalBinary(b, txs); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	return nil
}
