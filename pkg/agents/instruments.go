package agents

import (
	"bytes"
	"context"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processInstrumentCreation(ctx context.Context, transaction TransactionWithOutputs,
	creation *actions.InstrumentCreation) error {

	// First input must be the agent's locking script
	inputLockingScript, err := transaction.InputLockingScript(0)
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputLockingScript) {
		return nil // Not for this agent's contract
	}

	logger.Info(ctx, "Processing instrument creation")

	instrument, err := protocol.DeserializeInstrument([]byte(creation.InstrumentType),
		creation.InstrumentPayload)
	if err != nil {
		logger.Warn(ctx, "Instrument payload invalid: %s", err)
		return nil
	}

	a.contract.Lock()

	// Find existing matching instrument
	var existing *state.Instrument
	for _, i := range a.contract.Instruments {
		if bytes.Equal(i.InstrumentCode[:], creation.InstrumentCode) {
			existing = i
			break
		}
	}

	if existing != nil && creation.Timestamp < existing.Creation.Timestamp {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(creation.Timestamp)),
			logger.Timestamp("existing_timestamp", int64(existing.Creation.Timestamp)),
		}, "Older instrument creation")
	}

	txid := transaction.TxID()
	if existing == nil {
		newInstrument := &state.Instrument{
			ContractID:   state.CalculateContractID(a.contract.LockingScript),
			Creation:     creation,
			CreationTxID: &txid,
			Instrument:   instrument,
		}
		copy(newInstrument.InstrumentType[:], []byte(creation.InstrumentType))
		copy(newInstrument.InstrumentCode[:], creation.InstrumentCode)

		a.contract.Instruments = append(a.contract.Instruments, newInstrument)
	} else {
		copy(existing.InstrumentType[:], []byte(creation.InstrumentType))
		copy(existing.InstrumentCode[:], creation.InstrumentCode)
		existing.Creation = creation
		existing.CreationTxID = &txid
		existing.Instrument = instrument
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Timestamp("timestamp", int64(creation.Timestamp)),
	}, "Updated instrument creation")

	a.contract.Unlock()

	if err := a.contracts.Save(ctx, a.contract); err != nil {
		return errors.Wrap(err, "save contract")
	}

	return nil
}
