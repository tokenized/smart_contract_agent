package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/tokenized/channels"
	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/cacher"
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
}

type Transaction struct {
	Tx           *wire.MsgTx                 `bsor:"1" json:"tx"`
	State        wallet.TxState              `bsor:"2" json:"safe,omitempty"`
	MerkleProofs []*merkle_proof.MerkleProof `bsor:"3" json:"merkle_proofs,omitempty"`
	Outputs      []*wire.TxOut               `bsor:"4" json:"outputs,omitempty"` // outputs being spent by inputs in Tx

	IsProcessed bool `bsor:"5" json:"is_processed"`

	Ancestors channels.AncestorTxs `bsor:"-" json:"ancestors,omitempty"`

	sync.Mutex `bsor:"-"`
}

func NewTransactionCache(store storage.StreamStorage, fetcherCount, expireCount int,
	expiration, fetchTimeout time.Duration) (*TransactionCache, error) {

	cacher, err := cacher.NewCache(store, reflect.TypeOf(&Transaction{}), fetcherCount, expireCount,
		expiration, fetchTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "cacher")
	}

	return &TransactionCache{
		cacher: cacher,
	}, nil
}

func (c *TransactionCache) Run(ctx context.Context, interrupt <-chan interface{}) error {
	return c.cacher.Run(ctx, interrupt)
}

func (c *TransactionCache) Add(ctx context.Context, tx *Transaction) (*Transaction, error) {
	item, err := c.cacher.Add(ctx, tx)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	return item.(*Transaction), nil
}

func (c *TransactionCache) AddExpandedTx(ctx context.Context,
	etx *channels.ExpandedTx) (*Transaction, error) {

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

	txid := *tx.TxHash()
	itx := &Transaction{
		Tx:           tx,
		MerkleProofs: merkleProofs,
	}

	item, err := c.cacher.Add(ctx, itx)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	ftx := item.(*Transaction)
	if !ftx.AddMerkleProofs(merkleProofs) {
		return ftx, nil
	}

	if err := c.cacher.Save(ctx, ftx); err != nil {
		c.Release(ctx, txid)
		return nil, errors.Wrap(err, "save")
	}

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

	tx.Lock()
	defer tx.Lock()

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
	return true
}

func (c *TransactionCache) GetTxWithAncestors(ctx context.Context,
	txid bitcoin.Hash32) (*Transaction, error) {

	item, err := c.cacher.Get(ctx, TransactionPath(txid))
	if err != nil {
		return nil, errors.Wrap(err, "get tx")
	}

	if item == nil {
		return nil, nil
	}

	defer c.Release(ctx, txid)

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
			return nil, errors.Wrapf(err, "get input tx: %s", txin.PreviousOutPoint.Hash)
		}

		inputTx.Lock()
		atx = &channels.AncestorTx{
			Tx:           inputTx.Tx,
			MerkleProofs: inputTx.MerkleProofs,
		}
		inputTx.Unlock()

		tx.Ancestors = append(tx.Ancestors, atx)

		c.Release(ctx, txin.PreviousOutPoint.Hash)
	}

	return nil, nil
}

func (c *TransactionCache) Get(ctx context.Context, txid bitcoin.Hash32) (*Transaction, error) {
	item, err := c.cacher.Get(ctx, TransactionPath(txid))
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*Transaction), nil
}

func (c *TransactionCache) Save(ctx context.Context, transaction *Transaction) error {
	return c.cacher.Save(ctx, transaction)
}

func (c *TransactionCache) Release(ctx context.Context, txid bitcoin.Hash32) {
	c.cacher.Release(ctx, TransactionPath(txid))
}

func TransactionPath(txid bitcoin.Hash32) string {
	return fmt.Sprintf("%s/%s", txPath, txid)
}

func (tx *Transaction) TxID() bitcoin.Hash32 {
	return *tx.Tx.TxHash()
}

func (tx *Transaction) Path() string {
	tx.Lock()
	defer tx.Unlock()

	return TransactionPath(*tx.Tx.TxHash())
}

func (tx *Transaction) Serialize(w io.Writer) error {
	tx.Lock()
	b, err := bsor.MarshalBinary(tx)
	if err != nil {
		tx.Unlock()
		return errors.Wrap(err, "marshal")
	}
	tx.Unlock()

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

func (tx Transaction) InputLockingScript(index int) (bitcoin.Script, error) {
	if index >= len(tx.Tx.TxIn) {
		return nil, errors.New("Index out of range")
	}

	if index < len(tx.Outputs) {
		return tx.Outputs[index].LockingScript, nil
	}

	txin := tx.Tx.TxIn[index]

	parentTx := tx.Ancestors.GetTx(txin.PreviousOutPoint.Hash)
	if parentTx == nil {
		return nil, errors.Wrap(channels.MissingInput,
			"parent:"+txin.PreviousOutPoint.Hash.String())
	}

	ptx := parentTx.GetTx()
	if ptx == nil {
		return nil, errors.Wrap(channels.MissingInput,
			"parent tx:"+txin.PreviousOutPoint.Hash.String())
	}

	if txin.PreviousOutPoint.Index >= uint32(len(ptx.TxOut)) {
		return nil, errors.Wrap(channels.MissingInput, txin.PreviousOutPoint.String())
	}

	return ptx.TxOut[txin.PreviousOutPoint.Index].LockingScript, nil
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
