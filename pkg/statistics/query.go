package statistics

import (
	"context"
	"reflect"

	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/smart_contract_agent/internal/state"
)

func FetchContractValue(ctx context.Context, cache cacher.Cacher, contractHash state.ContractHash,
	time uint64) (*Statistics, error) {

	path := ContractPath(contractHash, time)
	typ := reflect.TypeOf(&Statistics{})

	return Get(ctx, cache, typ, path)
}

func FetchInstrumentValue(ctx context.Context, cache cacher.Cacher, contractHash state.ContractHash,
	instrumentCode state.InstrumentCode, time uint64) (*Statistics, error) {

	path := InstrumentPath(contractHash, instrumentCode, time)
	typ := reflect.TypeOf(&Statistics{})

	return Get(ctx, cache, typ, path)
}
