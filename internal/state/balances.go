package state

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/cacher"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

const (
	FreezeCode               = BalanceHoldingCode('F')
	DebitCode                = BalanceHoldingCode('D')
	DepositCode              = BalanceHoldingCode('P')
	MultiContractDebitCode   = BalanceHoldingCode('-')
	MultiContractDepositCode = BalanceHoldingCode('+')

	balanceVersion = uint8(0)
	balancePath    = "balances"
)

type Balance struct {
	LockingScript bitcoin.Script `bsor:"1" json:"locking_script"`
	Quantity      uint64         `bsor:"2" json:"quantity"`

	Holdings []*BalanceHolding `bsor:"3" json:"holdings,omitempty"`

	sync.Mutex `bsor:"-"`
}

type BalanceHoldingCode byte

type BalanceHolding struct {
	Code BalanceHoldingCode `bsor:"1" json:"code,omitempty"`

	Expires protocol.Timestamp `bsor:"1" json:"expires,omitempty"`
	Amount  uint64             `bsor:"1" json:"amount,omitempty"`
	TxID    *bitcoin.Hash32    `bsor:"1" json:"txID,omitempty"`
}

func NewBalanceCache(store storage.StreamStorage, fetcherCount, expireCount int,
	timeout time.Duration) (*cacher.Cache, error) {

	return cacher.NewCache(store, reflect.TypeOf(&Balance{}), balancePath, fetcherCount,
		expireCount, timeout)
}

func AddBalance(ctx context.Context, cache *cacher.Cache, b *Balance) (*Balance, error) {
	item, err := cache.Add(ctx, b)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	return item.(*Balance), nil
}

func GetBalance(ctx context.Context, cache *cacher.Cache, id bitcoin.Hash32) (*Balance, error) {
	item, err := cache.Get(ctx, id)
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*Balance), nil
}

func (b *Balance) ID() bitcoin.Hash32 {
	b.Lock()
	defer b.Unlock()

	return bitcoin.Hash32(sha256.Sum256(b.LockingScript))
}

func (b *Balance) Serialize(w io.Writer) error {
	b.Lock()
	bs, err := bsor.MarshalBinary(b)
	if err != nil {
		b.Unlock()
		return errors.Wrap(err, "marshal")
	}
	b.Unlock()

	if err := binary.Write(w, endian, balanceVersion); err != nil {
		return errors.Wrap(err, "version")
	}

	if err := binary.Write(w, endian, uint32(len(bs))); err != nil {
		return errors.Wrap(err, "size")
	}

	if _, err := w.Write(bs); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

func (b *Balance) Deserialize(r io.Reader) error {
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

	bs := make([]byte, size)
	if _, err := io.ReadFull(r, bs); err != nil {
		return errors.Wrap(err, "read")
	}

	b.Lock()
	defer b.Unlock()
	if _, err := bsor.UnmarshalBinary(bs, b); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	return nil
}

func (v BalanceHoldingCode) MarshalText() ([]byte, error) {
	s := v.String()
	if len(s) == 0 {
		return nil, fmt.Errorf("Unknown BalanceHoldingCode value \"%d\"", uint8(v))
	}

	return []byte(s), nil
}

func (v *BalanceHoldingCode) UnmarshalText(text []byte) error {
	return v.SetString(string(text))
}

func (v *BalanceHoldingCode) SetString(s string) error {
	switch s {
	case "freeze":
		*v = FreezeCode
	case "debit":
		*v = DebitCode
	case "deposit":
		*v = DepositCode
	case "multi_contract_debit":
		*v = MultiContractDebitCode
	case "multi_contract_deposit":
		*v = MultiContractDepositCode
	default:
		return fmt.Errorf("Unknown BalanceHoldingCode value \"%s\"", s)
	}

	return nil
}

func (v BalanceHoldingCode) String() string {
	switch v {
	case FreezeCode:
		return "freeze"
	case DebitCode:
		return "debit"
	case DepositCode:
		return "deposit"
	case MultiContractDebitCode:
		return "multi_contract_debit"
	case MultiContractDepositCode:
		return "multi_contract_deposit"
	default:
		return ""
	}
}
