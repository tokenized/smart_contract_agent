package commands

import (
	"context"
	"encoding/json"
	"fmt"

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

var Tx = &cobra.Command{
	Use:   "tx txid",
	Short: "Fetches information about a transaction",
	Args:  cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		ctx := logger.ContextWithLogger(context.Background(), true, false, "")

		txid, err := bitcoin.NewHash32FromStr(args[0])
		if err != nil {
			return errors.Wrap(err, "txid")
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

		transactions, err := transactions.NewTransactionCache(cache)
		if err != nil {
			return errors.Wrap(err, "create transaction cache")
		}

		tx, err := transactions.Get(ctx, *txid)
		if err != nil {
			return errors.Wrap(err, "get tx")
		}

		if tx == nil {
			return fmt.Errorf("Tx not found: %s", *txid)
		}
		defer transactions.Release(ctx, *txid)

		tx.Lock()
		defer tx.Unlock()

		js, err := json.MarshalIndent(tx, "", "  ")
		if err != nil {
			return errors.Wrap(err, "JSON marshal")
		}

		fmt.Printf("Tx State : %s\n", js)

		etx, err := transactions.ExpandedTx(ctx, tx)
		if err == nil {
			fmt.Printf("Tx : %s\n", etx)
		} else {
			fmt.Printf("Expanded tx failure : %s\n", err)
			fmt.Printf("Tx : %s\n", tx.Tx)
		}

		for actionIndex, txout := range tx.Tx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, cfg.IsTest)
			if err != nil {
				continue
			}

			displayAction(ctx, caches, tx, action, actionIndex)
		}

		return nil
	},
}

func displayAction(ctx context.Context, caches *state.Caches, tx *transactions.Transaction,
	action actions.Action, actionIndex int) {

	js, _ := json.MarshalIndent(action, "", "  ")
	fmt.Printf("Action : %s\n", js)

	switch act := action.(type) {
	case *actions.Transfer:
		displayTransfer(ctx, caches, tx, act, actionIndex)
	}
}

func displayTransfer(ctx context.Context, caches *state.Caches, tx *transactions.Transaction,
	transfer *actions.Transfer, actionIndex int) {

	for _, instrumentTransfer := range transfer.Instruments {
		contractOutput := tx.Tx.TxOut[instrumentTransfer.ContractIndex]

		instrumentID, _ := protocol.InstrumentIDForTransfer(instrumentTransfer)

		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], instrumentTransfer.InstrumentCode)

		fmt.Printf("Instrument (%s): %s", instrumentCode, instrumentID)

		for _, sender := range instrumentTransfer.InstrumentSenders {
			output, err := tx.InputOutput(int(sender.Index))
			if err != nil {
				fmt.Printf("Sender index %d : %s\n", sender.Index, err.Error())
				continue
			}

			balance, err := caches.Balances.Get(ctx, contractOutput.LockingScript, instrumentCode,
				output.LockingScript)
			if err != nil {
				fmt.Printf("Sender balance : %s\n", err.Error())
				continue
			}

			balance.Lock()
			js, _ := json.MarshalIndent(balance, "", "  ")
			balance.Unlock()
			fmt.Printf("Sender balance : %s\n", js)

			caches.Balances.Release(ctx, contractOutput.LockingScript, instrumentCode, balance)
		}

		for _, receiver := range instrumentTransfer.InstrumentReceivers {
			ra, err := bitcoin.DecodeRawAddress(receiver.Address)
			if err != nil {
				fmt.Printf("Receiver address : %s\n", err.Error())
			}

			ls, err := ra.LockingScript()
			if err != nil {
				fmt.Printf("Receiver locking script : %s\n", err.Error())
			}

			balance, err := caches.Balances.Get(ctx, contractOutput.LockingScript, instrumentCode,
				ls)
			if err != nil {
				fmt.Printf("Receiver balance : %s\n", err.Error())
				continue
			}

			balance.Lock()
			js, _ := json.MarshalIndent(balance, "", "  ")
			balance.Unlock()
			fmt.Printf("Receiver balance : %s\n", js)

			caches.Balances.Release(ctx, contractOutput.LockingScript, instrumentCode, balance)
		}
	}
}
