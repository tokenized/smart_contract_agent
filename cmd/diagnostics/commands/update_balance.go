package commands

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/tokenized/config"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var UpdateBalance = &cobra.Command{
	Use:   "update_balance contract_hash instrument_hash locking_script_hex quantity txid action_index timestamp",
	Short: "Updates the balance of an instrument for a locking script",
	RunE: func(c *cobra.Command, args []string) error {
		ctx := logger.ContextWithLogger(context.Background(), true, false, "")

		contractHash, err := bitcoin.NewHash32FromStr(args[0])
		if err != nil {
			return errors.Wrap(err, "contract hash")
		}

		instrumentHash, err := bitcoin.NewHash20FromStr(args[1])
		if err != nil {
			return errors.Wrap(err, "instrument hash")
		}

		lockingScript, err := hex.DecodeString(args[2])
		if err != nil {
			return errors.Wrap(err, "locking script")
		}

		quantity, err := strconv.ParseUint(args[3], 10, 64)
		if err != nil {
			return errors.Wrap(err, "quantity")
		}

		txid, err := bitcoin.NewHash32FromStr(args[4])
		if err != nil {
			return errors.Wrap(err, "txid")
		}

		actionIndex, err := strconv.ParseUint(args[5], 10, 32)
		if err != nil {
			return errors.Wrap(err, "action index")
		}

		timestamp, err := strconv.ParseUint(args[6], 10, 64)
		if err != nil {
			return errors.Wrap(err, "timestamp")
		}

		cfg := Config{}
		if err := config.LoadConfig(ctx, &cfg); err != nil {
			return errors.Wrap(err, "load config")
		}

		store, err := storage.CreateStreamStorage(cfg.Storage.Bucket, cfg.Storage.Root,
			cfg.Storage.MaxRetries, cfg.Storage.RetryDelay)
		if err != nil {
			return errors.Wrap(err, "create storage")
		}

		cache := cacher.NewSimpleCache(store)
		caches, err := state.NewCaches(cache)
		if err != nil {
			return errors.Wrap(err, "create caches")
		}

		if err := updateBalance(ctx, caches, *contractHash, *instrumentHash, lockingScript,
			uint64(quantity), *txid, uint32(actionIndex), uint64(timestamp),
			cfg.IsTest); err != nil {
			return errors.Wrap(err, "update balance")
		}

		return nil
	},
}

func updateBalance(ctx context.Context, caches *state.Caches, contractHash bitcoin.Hash32,
	instrumentHash bitcoin.Hash20, lockingScript bitcoin.Script, quantity uint64,
	txid bitcoin.Hash32, actionIndex uint32, timestamp uint64, isTest bool) error {

	contract, err := caches.Contracts.GetByHash(ctx, contractHash)
	if err != nil {
		return errors.Wrap(err, "get contract")
	}

	if contract == nil {
		fmt.Printf("Contract not found : %s\n", contractHash)
		return nil
	}

	contract.Lock()
	contractLockingScript := contract.LockingScript.Copy()
	defer caches.Contracts.Release(ctx, contractLockingScript)
	defer contract.Unlock()

	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], instrumentHash[:])

	instrument, err := caches.Instruments.Get(ctx, contractLockingScript, instrumentCode)
	if err != nil {
		return errors.Wrap(err, "get instrument")
	}

	if instrument == nil {
		fmt.Printf("Instrument not found : %s\n", instrumentCode)
		return nil
	}

	defer caches.Instruments.Release(ctx, contractLockingScript, instrumentCode)

	balance := &state.Balance{
		LockingScript: lockingScript,
		Quantity:      quantity,
		Timestamp:     timestamp,
		TxID:          &txid,
		ActionIndex: actionIndex,
	}
	balance.Initialize()

	addedBalance, err := caches.Balances.Add(ctx, contractLockingScript, instrumentCode, balance)
	if err != nil {
		return errors.Wrap(err, "add balance")
	}
	defer caches.Balances.Release(ctx, contractLockingScript, instrumentCode, addedBalance)

	if addedBalance != balance {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.JSON("old_balance", addedBalance),
		}, "Updating balance")

		addedBalance.Quantity = quantity
		addedBalance.Timestamp = timestamp
		addedBalance.TxID = &txid
		addedBalance.ActionIndex = actionIndex
		addedBalance.MarkModified()

		logger.InfoWithFields(ctx, []logger.Field{
			logger.JSON("new_balance", addedBalance),
		}, "Balance updated")
	} else {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.JSON("balance", addedBalance),
		}, "New balance added")
	}

	return nil
}
