package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/pkg/errors"
	"github.com/tokenized/config"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/statistics"
)

type Config struct {
	IsTest  bool           `json:"is_test" envconfig:"IS_TEST"`
	Storage storage.Config `json:"storage" envconfig:"STORAGE"`
}

var (
	argumentDescription = `statistics timestamp contract_hash [instrument_hash]`
)

func main() {
	if err := run(); err != nil {
		println(fmt.Sprintf("Failed : %s", err))
		os.Exit(1)
	}
}

func run() error {
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

	args := os.Args[1:]
	argCount := len(args)

	if argCount < 2 {
		return fmt.Errorf("Not enough arguments : got %d, need >= 1", argCount)
	}

	timestamp, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return errors.Wrap(err, "timestamp")
	}

	cHash, err := bitcoin.NewHash32FromStr(args[1])
	if err != nil {
		return errors.Wrap(err, "contract hash")
	}
	var contractHash state.ContractHash
	copy(contractHash[:], cHash[:])

	if argCount < 3 {
		stats, err := statistics.FetchContractValue(ctx, cache, contractHash, timestamp)
		if err != nil {
			return errors.Wrap(err, "fetch contract value")
		}

		js, err := json.MarshalIndent(stats, "", "  ")
		if err != nil {
			return errors.Wrap(err, "json marshal")
		}

		println(string(js))
		return nil
	}

	instrumentHash, err := bitcoin.NewHash20FromStr(args[2])
	if err != nil {
		return errors.Wrap(err, "instrument hash")
	}
	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], instrumentHash[:])

	stats, err := statistics.FetchInstrumentValue(ctx, cache, contractHash, instrumentCode,
		timestamp)
	if err != nil {
		return errors.Wrap(err, "fetch instrument value")
	}

	js, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return errors.Wrap(err, "json marshal")
	}

	println(string(js))
	return nil
}
