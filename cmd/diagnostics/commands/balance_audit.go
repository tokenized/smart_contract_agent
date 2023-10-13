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
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var BalanceAudit = &cobra.Command{
	Use:   "balances",
	Short: "Fetches all balances and checks them for pending adjustments",
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

		contractPaths, err := cache.List(ctx, "", "[a-f0-9]{64}/contract")
		if err != nil {
			return errors.Wrap(err, "list contracts")
		}

		for _, contractPath := range contractPaths {
			if err := auditContractBalances(ctx, caches, contractPath); err != nil {
				return errors.Wrapf(err, "list contract: %s", contractPath)
			}
		}

		return nil
	},
}

func auditContractBalances(ctx context.Context, caches *state.Caches, contractPath string) error {
	parts := strings.Split(contractPath, "/")
	if len(parts) != 2 {
		// fmt.Printf("Not contract path : %s\n", contractPath)
		return nil
	}

	if parts[1] != "contract" {
		// fmt.Printf("Not contract path : %s\n", contractPath)
		return nil
	}

	hash, err := bitcoin.NewHash32FromStr(parts[0])
	if err != nil {
		// fmt.Printf("Not contract path : %s\n", contractPath)
		return nil
	}

	contract, err := caches.Contracts.GetByHash(ctx, *hash)
	if err != nil {
		return errors.Wrap(err, "get contract")
	}

	if contract == nil {
		fmt.Printf("Contract not found : %s\n", contractPath)
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

		if err := auditInstrumentBalances(ctx, caches, lockingScript, instrumentCode); err != nil {
			return errors.Wrapf(err, "instrument: %d", i)
		}
	}

	return nil
}

func auditInstrumentBalances(ctx context.Context, caches *state.Caches,
	contractLockingScript bitcoin.Script, instrumentCode state.InstrumentCode) error {

	balances, err := caches.Balances.List(ctx, contractLockingScript, instrumentCode)
	if err != nil {
		return errors.Wrap(err, "list balances")
	}
	defer caches.Balances.ReleaseMulti(ctx, contractLockingScript, instrumentCode, balances)

	fmt.Printf("%d Balances\n", len(balances))

	for _, balance := range balances {
		balance.Lock()
		js, _ := json.MarshalIndent(balance, "", "  ")
		fmt.Printf("Balance: %s\n", js)
		balance.Unlock()
	}

	return nil
}
