package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tokenized/config"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var RepairPending = &cobra.Command{
	Use:   "repair_pending",
	Short: "Settles any pending balance adjustments where the settlement tx is already processed",
	RunE: func(c *cobra.Command, args []string) error {
		ctx := logger.ContextWithLogger(context.Background(), true, false, "")

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

		transactions, err := transactions.NewTransactionCache(cache)
		if err != nil {
			return errors.Wrap(err, "create transaction cache")
		}

		// List contracts
		contractPaths, err := cache.List(ctx, "", "[a-f0-9]{64}/contract")
		if err != nil {
			return errors.Wrap(err, "list contracts")
		}

		for _, contractPath := range contractPaths {
			parts := strings.Split(contractPath, "/")
			if len(parts) != 2 {
				continue
			}

			if parts[1] != "contract" {
				continue
			}

			hash, err := bitcoin.NewHash32FromStr(parts[0])
			if err != nil {
				continue
			}

			if err := settlePendingContractBalances(ctx, caches, transactions, *hash,
				cfg.IsTest); err != nil {
				return errors.Wrapf(err, "contract: %s", hash)
			}
		}

		return nil
	},
}

func settlePendingContractBalances(ctx context.Context, caches *state.Caches,
	transactions *transactions.TransactionCache, contractHash bitcoin.Hash32, isTest bool) error {

	contract, err := caches.Contracts.GetByHash(ctx, contractHash)
	if err != nil {
		return errors.Wrap(err, "get contract")
	}

	if contract == nil {
		fmt.Printf("Contract not found : %s\n", contractHash)
		return nil
	}

	contract.Lock()
	instrumentCount := contract.InstrumentCount
	lockingScript := contract.LockingScript.Copy()
	defer caches.Contracts.Release(ctx, lockingScript)
	defer contract.Unlock()

	ca, err := bitcoin.RawAddressFromLockingScript(lockingScript)
	if err != nil {
		return errors.Wrap(err, "contract address")
	}

	fmt.Printf("Contract (%d instruments): %s\n", instrumentCount, lockingScript)

	if instrumentCount == 0 {
		found := false
		for i := uint64(0); i < uint64(10); i++ {
			nextInstrumentCode := protocol.InstrumentCodeFromContract(ca, i)
			var instrumentCode state.InstrumentCode
			copy(instrumentCode[:], nextInstrumentCode[:])

			instrument, err := caches.Instruments.Get(ctx, lockingScript, instrumentCode)
			if err != nil {
				return errors.Wrap(err, "get instrument")
			}

			if instrument == nil {
				break
			}

			instrumentCount = i + 1
			found = true
			caches.Instruments.Release(ctx, lockingScript, instrumentCode)
		}

		if found {
			contract.InstrumentCount = instrumentCount
			contract.MarkModified()
			fmt.Printf("Updated instrument count : %d\n", instrumentCount)
		}
	}

	for i := uint64(0); i < instrumentCount; i++ {
		code := protocol.InstrumentCodeFromContract(ca, i)
		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], code[:])

		fmt.Printf("Instrument: %s\n", instrumentCode)

		if err := settlePendingInstrumentBalances(ctx, caches, transactions, lockingScript,
			instrumentCode, isTest); err != nil {
			return errors.Wrapf(err, "instrument: %d", i)
		}
	}

	return nil
}

func settlePendingInstrumentBalances(ctx context.Context, caches *state.Caches,
	transactions *transactions.TransactionCache, contractLockingScript bitcoin.Script,
	instrumentCode state.InstrumentCode, isTest bool) error {

	balances, err := caches.Balances.List(ctx, contractLockingScript, instrumentCode)
	if err != nil {
		return errors.Wrap(err, "list balances")
	}
	defer caches.Balances.ReleaseMulti(ctx, contractLockingScript, instrumentCode, balances)

	fmt.Printf("%d Balances\n", len(balances))

	for _, balance := range balances {
		if err := settlePendingBalance(ctx, caches, transactions, contractLockingScript,
			instrumentCode, balance, isTest); err != nil {

			balance.Lock()
			balanceLockingScript := balance.LockingScript.Copy()
			balance.Unlock()

			return errors.Wrapf(err, "balance: %x", []byte(balanceLockingScript))
		}
	}

	return nil
}

func settlePendingBalance(ctx context.Context, caches *state.Caches,
	transactions *transactions.TransactionCache, contractLockingScript bitcoin.Script,
	instrumentCode state.InstrumentCode, balance *state.Balance, isTest bool) error {

	balance.Lock()
	defer balance.Unlock()

	var adjustments []*state.BalanceAdjustment
	for _, adjustment := range balance.Adjustments {
		adjustments = append(adjustments, adjustment)
	}

	for i, adjustment := range adjustments {
		if err := settlePendingBalanceAdjustment(ctx, caches, transactions, contractLockingScript,
			instrumentCode, balance, adjustment, isTest); err != nil {
			return errors.Wrapf(err, "adjustment: %d", i)
		}
	}

	return nil
}

func settlePendingBalanceAdjustment(ctx context.Context, caches *state.Caches,
	transactions *transactions.TransactionCache, contractLockingScript bitcoin.Script,
	instrumentCode state.InstrumentCode, balance *state.Balance,
	adjustment *state.BalanceAdjustment, isTest bool) error {

	if adjustment.Code != state.MultiContractDebitCode &&
		adjustment.Code != state.MultiContractCreditCode {
		js, _ := json.MarshalIndent(adjustment, "", "  ")
		fmt.Printf("Skipping balance adjustment %x : %s\n", []byte(balance.LockingScript), js)
		return nil
	}

	if adjustment.TxID == nil {
		js, _ := json.MarshalIndent(adjustment, "", "  ")
		fmt.Printf("Skipping balance adjustment %x : %s\n", []byte(balance.LockingScript), js)
		return nil
	}
	transferTxID := *adjustment.TxID

	// Get transfer tx.
	transferTx, err := transactions.Get(ctx, transferTxID)
	if err != nil {
		return errors.Wrapf(err, "get transaction : %s", transferTxID)
	}

	if transferTx == nil {
		fmt.Printf("TxID not found : %s\n", transferTxID)
		js, _ := json.MarshalIndent(adjustment, "", "  ")
		fmt.Printf("Skipping balance adjustment %x : %s\n", []byte(balance.LockingScript), js)
		return nil
	}
	defer transactions.Release(ctx, transferTxID)

	transferTx.Lock()
	defer transferTx.Unlock()

	var transfer *actions.Transfer
	var transferIndex int
	for i, txout := range transferTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, isTest)
		if err != nil {
			continue
		}

		if t, ok := action.(*actions.Transfer); ok {
			transfer = t
			transferIndex = i
			break
		}
	}

	if transfer == nil {
		fmt.Printf("Transfer not found : %s\n", transferTxID)
		js, _ := json.MarshalIndent(adjustment, "", "  ")
		fmt.Printf("Skipping balance adjustment %x : %s\n", []byte(balance.LockingScript), js)
		return nil
	}

	processeds := transferTx.ContractProcessed(state.CalculateContractHash(contractLockingScript),
		transferIndex)
	var settlementTxID, rejectionTxID *bitcoin.Hash32
	var settlementTimestamp uint64
	for _, processed := range processeds {
		if processed.ResponseTxID == nil {
			continue
		}

		responseTx, err := transactions.Get(ctx, *processed.ResponseTxID)
		if err != nil {
			return errors.Wrap(err, "get response tx")
		}

		if responseTx == nil {
			continue
		}

		responseTx.Lock()

		for _, txout := range responseTx.Tx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, isTest)
			if err != nil {
				continue
			}

			if s, ok := action.(*actions.Settlement); ok {
				settlementTxID = processed.ResponseTxID
				settlementTimestamp = s.Timestamp
				break
			}

			if _, ok := action.(*actions.Rejection); ok {
				rejectionTxID = processed.ResponseTxID
				break
			}
		}

		responseTx.Unlock()
		transactions.Release(ctx, *processed.ResponseTxID)

		if settlementTxID != nil || rejectionTxID != nil {
			break
		}
	}

	if settlementTxID != nil {
		if balance.Settle(ctx, transferTxID, *settlementTxID, settlementTimestamp) {
			fmt.Printf("Transfer TxID : %s\n", transferTxID)
			fmt.Printf("Settlement TxID : %s\n", *settlementTxID)
			js, _ := json.MarshalIndent(adjustment, "", "  ")
			fmt.Printf("Settled balance adjustment %x : %s\n", []byte(balance.LockingScript), js)
		} else {
			fmt.Printf("Transfer TxID : %s\n", transferTxID)
			fmt.Printf("Settlement TxID : %s\n", *settlementTxID)
			js, _ := json.MarshalIndent(adjustment, "", "  ")
			fmt.Printf("Failed to settle balance adjustment %x : %s\n",
				[]byte(balance.LockingScript), js)
		}
	} else if rejectionTxID != nil {
		balance.CancelPending(transferTxID)
		fmt.Printf("Transfer TxID : %s\n", transferTxID)
			fmt.Printf("Rejection TxID : %s\n", *rejectionTxID)
		js, _ := json.MarshalIndent(adjustment, "", "  ")
		fmt.Printf("Cancelled balance adjustment %x : %s\n", []byte(balance.LockingScript), js)
	} else {
		fmt.Printf("Transfer response tx not found : %s\n", transferTxID)
		js, _ := json.MarshalIndent(adjustment, "", "  ")
		fmt.Printf("Skipping balance adjustment %x : %s\n", []byte(balance.LockingScript), js)
	}

	return nil
}
