package state

import (
	"fmt"
	"time"

	"github.com/tokenized/pkg/bitcoin"

	"github.com/pkg/errors"
)

var (
	BalanceUpdateTypeDebit  = BalanceUpdateType(1)
	BalanceUpdateTypeCredit = BalanceUpdateType(2)
	BalanceUpdateTypeFreeze = BalanceUpdateType(3)
	BalanceUpdateTypeThaw   = BalanceUpdateType(4)

	ErrUnknownBalanceUpdateType = errors.New("Unknown Balance Update Type")
)

type BalanceUpdateType uint8

type BalanceUpdate struct {
	Balance  *Balance
	Type     BalanceUpdateType
	Quantity uint64

	TxID  *bitcoin.Hash32
	Until uint64 // frozen until

	SettledQuantity uint64
}

type BalanceUpdates struct {
	Updates []*BalanceUpdate
	Now     uint64
}

func (us *BalanceUpdates) Execute() error {
	for _, u := range us.Updates {
		u.Balance.Lock()
	}

	// Get timestamp after locking all the balances. This ensures the timestamp is in order if
	// multiple transactions are being processed concurrently.
	now := uint64(time.Now().UnixNano())

	for i, u := range us.Updates {
		if err := u.Execute(now); err != nil {
			for _, u := range us.Updates {
				u.Balance.RevertPending()
				u.Balance.Unlock()
			}

			return errors.Wrapf(err, "%d", i)
		}
	}

	for _, u := range us.Updates {
		u.Balance.Unlock()
	}

	us.Now = now

	return nil
}

func (u *BalanceUpdate) Execute(now uint64) error {
	switch u.Type {
	case BalanceUpdateTypeDebit:
		if err := u.Balance.AddPendingDebit(u.Quantity, now); err != nil {
			return err
		}

		u.SettledQuantity = u.Balance.SettlePendingQuantity()
		return nil

	case BalanceUpdateTypeCredit:
		if err := u.Balance.AddPendingCredit(u.Quantity, now); err != nil {
			return err
		}

		u.SettledQuantity = u.Balance.SettlePendingQuantity()
		return nil

	case BalanceUpdateTypeFreeze:
		u.SettledQuantity = u.Balance.AddFreeze(*u.TxID, u.Quantity, u.Until)
		return nil
	}

	return errors.Wrap(ErrUnknownBalanceUpdateType, u.Type.String())
}

func (v BalanceUpdateType) MarshalText() ([]byte, error) {
	s := v.String()
	if len(s) == 0 {
		return nil, fmt.Errorf("Unknown BalanceUpdateType value \"%d\"", uint8(v))
	}

	return []byte(s), nil
}

func (v *BalanceUpdateType) UnmarshalText(text []byte) error {
	return v.SetString(string(text))
}

func (v *BalanceUpdateType) SetString(s string) error {
	switch s {
	case "debit":
		*v = BalanceUpdateTypeDebit
	case "credit":
		*v = BalanceUpdateTypeCredit
	case "freeze":
		*v = BalanceUpdateTypeFreeze
	case "thaw":
		*v = BalanceUpdateTypeThaw
	default:
		*v = 0
		return fmt.Errorf("Unknown BalanceUpdateType value \"%s\"", s)
	}

	return nil
}

func (v BalanceUpdateType) String() string {
	switch v {
	case BalanceUpdateTypeDebit:
		return "debit"
	case BalanceUpdateTypeCredit:
		return "credit"
	case BalanceUpdateTypeFreeze:
		return "freeze"
	case BalanceUpdateTypeThaw:
		return "thaw"
	default:
		return fmt.Sprintf("invalid(%d)", uint8(v))
	}
}
