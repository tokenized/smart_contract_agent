package transactions

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	ci "github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

const (
	TxStatePending   = TxState(0) // Initial time period to see double spend attempts has not passed.
	TxStateSafe      = TxState(1) // No conflicting txs seen for initial time period.
	TxStateUnsafe    = TxState(2) // Conflicting tx seen, but not confirmed.
	TxStateCancelled = TxState(4) // Conflicting tx confirmed.

	txVersion = uint8(0)
	txPath    = "txs"
)

var (
	ErrNegativeFee = errors.New("Negative Fee")

	endian = binary.LittleEndian
)

type TxState uint8

type TransactionCache struct {
	cacher ci.Cacher
	typ    reflect.Type
}

type Processed struct {
	// Contract is the hash of the contract that responded.
	Contract state.ContractHash `bsor:"1" json:"contract"`

	// OutputIndex is the index of the output containing the action that was responded to.
	OutputIndex int `bsor:"2" json:"output_index"`

	// ResponseTxID is the txid of the response.
	ResponseTxID *bitcoin.Hash32 `bsor:"3" json:"response_txid"`
}

type Transaction struct {
	Tx           *wire.MsgTx               `bsor:"1" json:"tx"`
	State        TxState                   `bsor:"2" json:"state,omitempty"`
	MerkleProofs merkle_proof.MerkleProofs `bsor:"3" json:"merkle_proofs,omitempty"`
	SpentOutputs expanded_tx.Outputs       `bsor:"4" json:"spent_outputs,omitempty"` // outputs being spent by inputs in Tx

	Processed []*Processed `bsor:"6" json:"processed,omitempty"`

	// ConflictingTxIDs lists txs that have conflicting inputs and will "double spend" this tx if
	// confirmed.
	// ConflictingTxIDs []bitcoin.Hash32 `bsor:"7" json:"conflicting_txids"`

	Ancestors expanded_tx.AncestorTxs `bsor:"-" json:"ancestors,omitempty"`

	isProcessing []state.ContractHash
	isModified   bool
	sync.Mutex   `bsor:"-"`
}

func NewTransactionCache(cache ci.Cacher) (*TransactionCache, error) {
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
	if _, ok := itemInterface.(ci.Value); !ok {
		return nil, errors.New("Type must implement CacheValue")
	}

	return &TransactionCache{
		cacher: cache,
		typ:    typ,
	}, nil
}

func (c *TransactionCache) Save(ctx context.Context, tx *Transaction) {
	c.cacher.Save(ctx, TransactionPath(tx.GetTxID()), tx)
}

func (c *TransactionCache) Add(ctx context.Context, tx *Transaction) (*Transaction, error) {
	item, err := c.cacher.Add(ctx, c.typ, TransactionPath(tx.GetTxID()), tx)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	addedTx := item.(*Transaction)
	if addedTx != tx {
		// Merge tx into addedTx
		addedTx.Lock()

		if len(addedTx.SpentOutputs) == 0 {
			addedTx.SpentOutputs = tx.SpentOutputs
			addedTx.MarkModified()
		}

		addedTx.AddMerkleProofs(tx.MerkleProofs)

		addedTx.Unlock()
	}

	return addedTx, nil
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
	tx, err := c.AddRawWithOutputs(ctx, etx.Tx, etx.SpentOutputs)
	if err != nil {
		return nil, errors.Wrapf(err, "add: %s", txid)
	}

	tx.Lock()
	tx.addAncestors(etx.Ancestors)
	tx.Unlock()

	return tx, nil
}

func (c *TransactionCache) AddRawWithOutputs(ctx context.Context, tx *wire.MsgTx,
	spentOutputs []*expanded_tx.Output) (*Transaction, error) {

	itx := &Transaction{
		Tx:           tx,
		SpentOutputs: spentOutputs,
	}

	item, err := c.cacher.Add(ctx, c.typ, TransactionPath(itx.GetTxID()), itx)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	addedTx := item.(*Transaction)
	if itx != addedTx {
		// Not a new tx
		addedTx.Lock()
		if len(addedTx.SpentOutputs) == 0 {
			addedTx.SpentOutputs = spentOutputs
			addedTx.isModified = true
		}
		addedTx.Unlock()
	}

	return addedTx, nil
}

func (c *TransactionCache) AddRaw(ctx context.Context, tx *wire.MsgTx,
	merkleProofs merkle_proof.MerkleProofs) (*Transaction, error) {

	txCopy := tx.Copy()
	itx := &Transaction{
		Tx:           &txCopy,
		MerkleProofs: merkleProofs.Copy(),
	}

	item, err := c.cacher.Add(ctx, c.typ, TransactionPath(itx.GetTxID()), itx)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	addedTx := item.(*Transaction)
	if itx != addedTx {
		// Not a new tx
		addedTx.Lock()
		addedTx.AddMerkleProofs(merkleProofs)
		addedTx.Unlock()
	}

	return addedTx, nil
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

func (c *TransactionCache) ExpandedTx(ctx context.Context,
	transaction *Transaction) (*expanded_tx.ExpandedTx, error) {

	if err := c.PopulateAncestors(ctx, transaction); err != nil {
		return nil, errors.Wrap(err, "populate ancestors")
	}

	txCopy := transaction.Tx.Copy()
	return &expanded_tx.ExpandedTx{
		Tx:           &txCopy,
		Ancestors:    transaction.Ancestors.Copy(),
		SpentOutputs: transaction.SpentOutputs.Copy(),
	}, nil
}

func (c *TransactionCache) GetExpandedTx(ctx context.Context,
	txid bitcoin.Hash32) (*expanded_tx.ExpandedTx, error) {

	transaction, err := c.GetTxWithAncestors(ctx, txid)
	if err != nil {
		return nil, errors.Wrap(err, "get tx")
	}

	if transaction == nil {
		return nil, nil
	}

	defer c.Release(ctx, txid)
	txCopy := transaction.Tx.Copy()
	return &expanded_tx.ExpandedTx{
		Tx:           &txCopy,
		Ancestors:    transaction.Ancestors.Copy(),
		SpentOutputs: transaction.SpentOutputs.Copy(),
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

	transaction := item.(*Transaction)
	transaction.Lock()
	if err := c.PopulateAncestors(ctx, transaction); err != nil {
		transaction.Unlock()
		c.Release(ctx, txid)
		return nil, errors.Wrap(err, "populate ancestors")
	}
	transaction.Unlock()
	return transaction, nil
}

func (c *TransactionCache) PopulateAncestors(ctx context.Context, transaction *Transaction) error {
	// Fetch ancestors
	for _, txin := range transaction.Tx.TxIn {
		atx := transaction.Ancestors.GetTx(txin.PreviousOutPoint.Hash)
		if atx != nil {
			continue // already have this ancestor
		}

		inputTx, err := c.Get(ctx, txin.PreviousOutPoint.Hash)
		if err != nil {
			return errors.Wrapf(err, "get input tx: %s", txin.PreviousOutPoint.Hash)
		}
		if inputTx == nil {
			continue
		}

		inputTx.Lock()
		txCopy := inputTx.Tx.Copy()
		atx = &expanded_tx.AncestorTx{
			Tx:           &txCopy,
			MerkleProofs: inputTx.MerkleProofs.Copy(),
		}
		inputTx.Unlock()

		transaction.Ancestors = append(transaction.Ancestors, atx)

		c.Release(ctx, txin.PreviousOutPoint.Hash)
	}

	return nil
}

func (tx *Transaction) addAncestors(ancestors expanded_tx.AncestorTxs) {
	for _, ancestor := range ancestors {
		atxid := ancestor.GetTxID()
		if atxid == nil {
			continue
		}

		parentTx := tx.Ancestors.GetTx(*atxid)
		if parentTx == nil {
			tx.Ancestors = append(tx.Ancestors, ancestor)
		}
	}
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

func (c *TransactionCache) GetMulti(ctx context.Context,
	txids []bitcoin.Hash32) ([]*Transaction, error) {

	paths := make([]string, len(txids))
	for i, txid := range txids {
		paths[i] = TransactionPath(txid)
	}

	items, err := c.cacher.GetMulti(ctx, c.typ, paths)
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	txs := make([]*Transaction, len(items))
	for i, item := range items {
		if item == nil {
			txs[i] = nil
			continue
		}

		txs[i] = item.(*Transaction)
	}

	return txs, nil
}

func (c *TransactionCache) Release(ctx context.Context, txid bitcoin.Hash32) {
	c.cacher.Release(ctx, TransactionPath(txid))
}

func (tx *Transaction) SetIsProcessing(contract state.ContractHash) bool {
	for _, c := range tx.isProcessing {
		if c.Equal(contract) {
			return false
		}
	}

	tx.isProcessing = append(tx.isProcessing, contract)
	return true
}

func (tx *Transaction) ClearIsProcessing(contract state.ContractHash) {
	for i, c := range tx.isProcessing {
		if c.Equal(contract) {
			tx.isProcessing = append(tx.isProcessing[:i], tx.isProcessing[i+1:]...)
			return
		}
	}
}

func (tx *Transaction) SetProcessed(contract state.ContractHash, outputIndex int) bool {
	for _, processed := range tx.Processed {
		if processed.Contract.Equal(contract) && processed.OutputIndex == outputIndex {
			return false
		}
	}

	tx.Processed = append(tx.Processed, &Processed{
		Contract:    contract,
		OutputIndex: outputIndex,
	})
	tx.isModified = true
	return true
}

func (tx *Transaction) AddResponseTxID(contract state.ContractHash, outputIndex int,
	txid bitcoin.Hash32) bool {

	for _, processed := range tx.Processed {
		if processed.Contract.Equal(contract) && processed.OutputIndex == outputIndex &&
			processed.ResponseTxID != nil && processed.ResponseTxID.Equal(&txid) {
			return false
		}
	}

	tx.Processed = append(tx.Processed, &Processed{
		Contract:     contract,
		OutputIndex:  outputIndex,
		ResponseTxID: &txid,
	})
	tx.isModified = true
	return true
}

func (tx *Transaction) ContractProcessed(contract state.ContractHash,
	outputIndex int) []*Processed {

	var result []*Processed
	for _, r := range tx.Processed {
		if r.Contract.Equal(contract) && r.OutputIndex == outputIndex {
			result = append(result, r)
		}
	}

	return result
}

func (tx *Transaction) GetContractProcessed(contract state.ContractHash,
	outputIndex int) []*Processed {
	tx.Lock()
	defer tx.Unlock()
	return tx.ContractProcessed(contract, outputIndex)
}

func TransactionPath(txid bitcoin.Hash32) string {
	return fmt.Sprintf("%s/%s", txPath, txid)
}

func (tx *Transaction) TxID() bitcoin.Hash32 {
	return *tx.Tx.TxHash()
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

func (t *Transaction) CacheCopy() ci.Value {
	result := &Transaction{
		State:        t.State,
		MerkleProofs: t.MerkleProofs.Copy(),
		SpentOutputs: t.SpentOutputs.Copy(),
		Processed:    make([]*Processed, len(t.Processed)),
		Ancestors:    t.Ancestors.Copy(),
	}

	if t.Tx != nil {
		txCopy := t.Tx.Copy()
		result.Tx = &txCopy
	}

	for i, response := range t.Processed {
		r := response.Copy()
		result.Processed[i] = &r
	}

	return result
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
	c := tx.Tx.TxIn[index].Copy()
	return &c
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

	c := ptx.TxOut[txin.PreviousOutPoint.Index].Copy()
	return &c, nil
}

func (tx Transaction) OutputCount() int {
	return len(tx.Tx.TxOut)
}

func (tx Transaction) Output(index int) *wire.TxOut {
	c := tx.Tx.TxOut[index].Copy()
	return &c
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
	txCopy := tx.Tx.Copy()
	return &txCopy
}

func (tx *Transaction) GetTxID() bitcoin.Hash32 {
	tx.Lock()
	defer tx.Unlock()
	return *tx.Tx.TxHash()
}

func (tx *Transaction) CalculateFee() (uint64, error) {
	inputValue := uint64(0)
	for index := range tx.Tx.TxIn {
		inputOutput, err := tx.InputOutput(index)
		if err != nil {
			return 0, errors.Wrapf(err, "input %d", index)
		}

		inputValue += inputOutput.Value
	}

	outputValue := uint64(0)
	for _, txout := range tx.Tx.TxOut {
		outputValue += txout.Value
	}

	if outputValue > inputValue {
		return 0, ErrNegativeFee
	}

	return inputValue - outputValue, nil
}

func (tx *Transaction) Size() uint64 {
	return uint64(tx.Tx.SerializeSize())
}

func (r Processed) Copy() Processed {
	var result Processed
	copy(result.Contract[:], r.Contract[:])
	result.OutputIndex = r.OutputIndex
	if r.ResponseTxID != nil {
		result.ResponseTxID = &bitcoin.Hash32{}
		copy(result.ResponseTxID[:], r.ResponseTxID[:])
	}
	return result
}

func (v TxState) MarshalText() ([]byte, error) {
	s := v.String()
	if len(s) == 0 {
		return nil, fmt.Errorf("Unknown TxState value \"%d\"", uint8(v))
	}

	return []byte(s), nil
}

func (v *TxState) UnmarshalText(text []byte) error {
	return v.SetString(string(text))
}

func (v *TxState) SetString(s string) error {
	parts := strings.Split(s, "|")
	value := TxStatePending
	for _, part := range parts {
		switch s {
		case "pending":
			value |= TxStatePending
		case "safe":
			value |= TxStateSafe
		case "unsafe":
			value |= TxStateUnsafe
		case "cancelled":
			value |= TxStateCancelled
		default:
			*v = 0
			return fmt.Errorf("Unknown TxState value \"%s\"", part)
		}
	}

	*v = value
	return nil
}

func (v TxState) String() string {
	var values []string
	if v&TxStatePending != 0 {
		values = append(values, "pending")
	}
	if v&TxStateSafe != 0 {
		values = append(values, "safe")
	}
	if v&TxStateUnsafe != 0 {
		values = append(values, "unsafe")
	}
	if v&TxStateCancelled != 0 {
		values = append(values, "cancelled")
	}

	return strings.Join(values, "|")
}
