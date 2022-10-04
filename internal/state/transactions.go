package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/tokenized/cacher"
	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

const (
	txVersion = uint8(0)
	txPath    = "txs"
)

type TransactionCache struct {
	cacher *cacher.Cache
	typ    reflect.Type
}

type Transaction struct {
	Tx           *wire.MsgTx                 `bsor:"1" json:"tx"`
	State        wallet.TxState              `bsor:"2" json:"state,omitempty"`
	MerkleProofs []*merkle_proof.MerkleProof `bsor:"3" json:"merkle_proofs,omitempty"`
	SpentOutputs []*expanded_tx.Output       `bsor:"4" json:"spent_outputs,omitempty"` // outputs being spent by inputs in Tx

	IsProcessed bool `bsor:"5" json:"is_processed"`

	Ancestors expanded_tx.AncestorTxs `bsor:"-" json:"ancestors,omitempty"`

	isModified bool
	sync.Mutex `bsor:"-"`
}

func NewTransactionCache(cache *cacher.Cache) (*TransactionCache, error) {
	typ := reflect.TypeOf(&Transaction{})

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

	return &TransactionCache{
		cacher: cache,
		typ:    typ,
	}, nil
}

func (c *TransactionCache) Save(ctx context.Context, tx *Transaction) {
	c.cacher.Save(ctx, tx)
}

func (c *TransactionCache) Add(ctx context.Context, tx *Transaction) (*Transaction, error) {
	item, err := c.cacher.Add(ctx, c.typ, tx)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	return item.(*Transaction), nil
}

func (c *TransactionCache) AddExpandedTx(ctx context.Context,
	etx *expanded_tx.ExpandedTx) (*Transaction, error) {

	for _, atx := range etx.Ancestors {
		atxid := *atx.Tx.TxHash()
		if _, err := c.AddRaw(ctx, atx.Tx, atx.MerkleProofs); err != nil {
			return nil, errors.Wrapf(err, "ancestor: %s", atxid)
		}
		c.Release(ctx, atxid)
	}

	txid := *etx.Tx.TxHash()
	tx, err := c.AddRaw(ctx, etx.Tx, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "add: %s", txid)
	}
	c.Release(ctx, txid)

	return tx, nil
}

func (c *TransactionCache) AddRaw(ctx context.Context, tx *wire.MsgTx,
	merkleProofs []*merkle_proof.MerkleProof) (*Transaction, error) {

	itx := &Transaction{
		Tx:           tx,
		MerkleProofs: merkleProofs,
	}

	item, err := c.cacher.Add(ctx, c.typ, itx)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	ftx := item.(*Transaction)
	ftx.AddMerkleProofs(merkleProofs)
	return ftx, nil
}

func (tx *Transaction) AddMerkleProofs(merkleProofs []*merkle_proof.MerkleProof) bool {
	modified := false
	for _, mp := range merkleProofs {
		if tx.AddMerkleProof(mp) {
			modified = true
		}
	}

	return modified
}

func (tx *Transaction) AddMerkleProof(merkleProof *merkle_proof.MerkleProof) bool {
	blockHash := merkleProof.GetBlockHash()
	if blockHash == nil {
		return false
	}

	for _, mp := range tx.MerkleProofs {
		bh := mp.GetBlockHash()
		if bh == nil {
			continue
		}

		if bh.Equal(blockHash) {
			return false // already have this merkle proof
		}
	}

	mp := *merkleProof
	if mp.Tx != nil {
		mp.TxID = mp.Tx.TxHash()
		mp.Tx = nil
	}

	tx.MerkleProofs = append(tx.MerkleProofs, &mp)
	tx.MarkModified()
	return true
}

func (tx *Transaction) ExpandedTx(ctx context.Context) (*expanded_tx.ExpandedTx, error) {
	return &expanded_tx.ExpandedTx{
		Tx:           tx.Tx,
		Ancestors:    tx.Ancestors,
		SpentOutputs: tx.SpentOutputs,
	}, nil
}

func (c *TransactionCache) GetExpandedTx(ctx context.Context,
	txid bitcoin.Hash32) (*expanded_tx.ExpandedTx, error) {

	tx, err := c.GetTxWithAncestors(ctx, txid)
	if err != nil {
		return nil, errors.Wrap(err, "get tx")
	}

	if tx == nil {
		return nil, nil
	}

	return &expanded_tx.ExpandedTx{
		Tx:           tx.Tx,
		Ancestors:    tx.Ancestors,
		SpentOutputs: tx.SpentOutputs,
	}, nil
}

func (c *TransactionCache) GetTxWithAncestors(ctx context.Context,
	txid bitcoin.Hash32) (*Transaction, error) {

	item, err := c.cacher.Get(ctx, c.typ, TransactionPath(txid))
	if err != nil {
		return nil, errors.Wrap(err, "get tx")
	}

	if item == nil {
		return nil, nil
	}

	tx := item.(*Transaction)
	tx.Lock()
	defer tx.Unlock()

	// Fetch ancestors
	for _, txin := range tx.Tx.TxIn {
		atx := tx.Ancestors.GetTx(txin.PreviousOutPoint.Hash)
		if atx != nil {
			continue // already have this ancestor
		}

		inputTx, err := c.Get(ctx, txin.PreviousOutPoint.Hash)
		if err != nil {
			c.Release(ctx, txid)
			return nil, errors.Wrapf(err, "get input tx: %s", txin.PreviousOutPoint.Hash)
		}
		if inputTx == nil {
			continue
		}

		inputTx.Lock()
		atx = &expanded_tx.AncestorTx{
			Tx:           inputTx.Tx,
			MerkleProofs: inputTx.MerkleProofs,
		}
		inputTx.Unlock()

		tx.Ancestors = append(tx.Ancestors, atx)

		c.Release(ctx, txin.PreviousOutPoint.Hash)
	}

	return tx, nil
}

func (c *TransactionCache) Get(ctx context.Context, txid bitcoin.Hash32) (*Transaction, error) {
	item, err := c.cacher.Get(ctx, c.typ, TransactionPath(txid))
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*Transaction), nil
}

func (c *TransactionCache) Release(ctx context.Context, txid bitcoin.Hash32) {
	c.cacher.Release(ctx, TransactionPath(txid))
}

func (tx *Transaction) SetProcessed() {
	if tx.IsProcessed {
		return
	}

	tx.IsProcessed = true
	tx.isModified = true
}

func TransactionPath(txid bitcoin.Hash32) string {
	return fmt.Sprintf("%s/%s", txPath, txid)
}

func (tx *Transaction) TxID() bitcoin.Hash32 {
	return *tx.Tx.TxHash()
}

func (tx *Transaction) Path() string {
	return TransactionPath(*tx.Tx.TxHash())
}

func (tx *Transaction) MarkModified() {
	tx.isModified = true
}

func (tx *Transaction) ClearModified() {
	tx.isModified = false
}

func (tx *Transaction) IsModified() bool {
	return tx.isModified
}

func (tx *Transaction) Serialize(w io.Writer) error {
	b, err := bsor.MarshalBinary(tx)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := binary.Write(w, endian, txVersion); err != nil {
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

func (tx *Transaction) Deserialize(r io.Reader) error {
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

	tx.Lock()
	defer tx.Unlock()
	if _, err := bsor.UnmarshalBinary(b, tx); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	return nil
}

func (tx Transaction) InputCount() int {
	return len(tx.Tx.TxIn)
}

func (tx Transaction) Input(index int) *wire.TxIn {
	return tx.Tx.TxIn[index]
}

func (tx Transaction) InputOutput(index int) (*wire.TxOut, error) {
	if index >= len(tx.Tx.TxIn) {
		return nil, errors.New("Index out of range")
	}

	if index < len(tx.SpentOutputs) {
		output := tx.SpentOutputs[index]
		return &wire.TxOut{
			LockingScript: output.LockingScript,
			Value:         output.Value,
		}, nil
	}

	txin := tx.Tx.TxIn[index]

	parentTx := tx.Ancestors.GetTx(txin.PreviousOutPoint.Hash)
	if parentTx == nil {
		return nil, errors.Wrap(expanded_tx.MissingInput,
			"parent:"+txin.PreviousOutPoint.Hash.String())
	}

	ptx := parentTx.GetTx()
	if ptx == nil {
		return nil, errors.Wrap(expanded_tx.MissingInput,
			"parent tx:"+txin.PreviousOutPoint.Hash.String())
	}

	if txin.PreviousOutPoint.Index >= uint32(len(ptx.TxOut)) {
		return nil, errors.Wrap(expanded_tx.MissingInput, txin.PreviousOutPoint.String())
	}

	return ptx.TxOut[txin.PreviousOutPoint.Index], nil
}

func (tx Transaction) OutputCount() int {
	return len(tx.Tx.TxOut)
}

func (tx Transaction) Output(index int) *wire.TxOut {
	return tx.Tx.TxOut[index]
}

func (tx Transaction) ParseActions(isTest bool) []actions.Action {
	var result []actions.Action
	for _, txout := range tx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, isTest)
		if err != nil {
			continue
		}

		result = append(result, action)
	}

	return result
}

func (tx Transaction) GetMsgTx() *wire.MsgTx {
	return tx.Tx
}

func (tx *Transaction) GetTxID() bitcoin.Hash32 {
	tx.Lock()
	defer tx.Unlock()
	return *tx.Tx.TxHash()
}
