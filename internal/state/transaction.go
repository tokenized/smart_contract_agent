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

	"github.com/pkg/errors"
)

const (
	txVersion = uint8(0)
	txPath    = "txs"
)

type Transaction struct {
	Tx           *wire.MsgTx                 `bsor:"1" json:"tx"`
	State        wallet.TxState              `bsor:"2" json:"safe,omitempty"`
	MerkleProofs []*merkle_proof.MerkleProof `bsor:"3" json:"merkle_proofs,omitempty"`

	Ancestors channels.AncestorTxs `bsor:"-" json:"ancestors,omitempty"`

	sync.Mutex `bsor:"-"`
}

func NewTxCache(store storage.StreamStorage, expireCount int,
	timeout time.Duration) (*cacher.Cache, error) {

	return cacher.NewCache(store, reflect.TypeOf(&Transaction{}), txPath, expireCount, timeout)
}

func AddExpandedTx(ctx context.Context, cache *cacher.Cache,
	etx *channels.ExpandedTx) (*Transaction, error) {

	for _, atx := range etx.Ancestors {
		atxid := *atx.Tx.TxHash()
		if _, err := AddTx(ctx, cache, atx.Tx, atx.MerkleProofs); err != nil {
			return nil, errors.Wrapf(err, "ancestor: %s", atxid)
		}
		cache.Release(ctx, atxid)
	}

	txid := *etx.Tx.TxHash()
	tx, err := AddTx(ctx, cache, etx.Tx, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "add: %s", txid)
	}
	cache.Release(ctx, txid)

	return tx, nil
}

func AddTx(ctx context.Context, cache *cacher.Cache, tx *wire.MsgTx,
	merkleProofs []*merkle_proof.MerkleProof) (*Transaction, error) {

	txid := *tx.TxHash()
	itx := &Transaction{
		Tx:           tx,
		MerkleProofs: merkleProofs,
	}

	item, err := cache.Add(ctx, itx)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	ftx := item.(*Transaction)
	if !ftx.AddMerkleProofs(merkleProofs) {
		return ftx, nil
	}

	if err := cache.Save(ctx, ftx); err != nil {
		cache.Release(ctx, txid)
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

func GetExpandedTx(ctx context.Context, cache *cacher.Cache,
	txid bitcoin.Hash32) (*channels.ExpandedTx, error) {

	item, err := cache.Get(ctx, txid)
	if err != nil {
		return nil, errors.Wrap(err, "get tx")
	}

	if item == nil {
		return nil, nil
	}

	defer cache.Release(ctx, txid)

	tx := item.(*Transaction)
	tx.Lock()
	defer tx.Unlock()

	etx := &channels.ExpandedTx{
		Tx:        tx.Tx,
		Ancestors: tx.Ancestors,
	}

	// Fetch ancestors
	for _, txin := range tx.Tx.TxIn {
		atx := tx.Ancestors.GetTx(txin.PreviousOutPoint.Hash)
		if atx != nil {
			continue // already have this ancestor
		}

		inputItem, err := cache.Get(ctx, txin.PreviousOutPoint.Hash)
		if err != nil {
			return nil, errors.Wrapf(err, "get input tx: %s", txin.PreviousOutPoint.Hash)
		}

		inputTx := inputItem.(*Transaction)

		inputTx.Lock()
		atx = &channels.AncestorTx{
			Tx:           inputTx.Tx,
			MerkleProofs: inputTx.MerkleProofs,
		}
		inputTx.Unlock()

		etx.Ancestors = append(etx.Ancestors, atx)

		cache.Release(ctx, txin.PreviousOutPoint.Hash)
	}

	return nil, nil
}

func GetTx(ctx context.Context, cache *cacher.Cache, txid bitcoin.Hash32) (*Transaction, error) {
	item, err := cache.Get(ctx, txid)
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*Transaction), nil
}

func (tx *Transaction) ID() bitcoin.Hash32 {
	tx.Lock()
	defer tx.Unlock()

	return *tx.Tx.TxHash()
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
