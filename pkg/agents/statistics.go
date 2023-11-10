package agents

import (
	"context"

	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/statistics"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) updateRequestStats(ctx context.Context, requestTx, responseTx *wire.MsgTx,
	requestOutputIndex, contractFeeOutputIndex int, wasRejected bool,
	now uint64) error {

	return a.updateRequestStatsFull(ctx, requestTx, responseTx, requestOutputIndex,
		contractFeeOutputIndex, nil, wasRejected, now)
}

func (a *Agent) updateRequestStatsTransfer(ctx context.Context, requestTx, responseTx *wire.MsgTx,
	requestOutputIndex, contractFeeOutputIndex int, transferFees []*wire.TxOut, wasRejected bool,
	now uint64) error {

	return a.updateRequestStatsFull(ctx, requestTx, responseTx, requestOutputIndex,
		contractFeeOutputIndex, transferFees, wasRejected, now)
}

func (a *Agent) updateRequestStatsFull(ctx context.Context, requestTx, responseTx *wire.MsgTx,
	requestOutputIndex, contractFeeOutputIndex int, transferFees []*wire.TxOut, wasRejected bool,
	now uint64) error {

	us := a.updateStats.Load()
	if us == nil {
		return nil
	}
	updateStats := us.(statistics.AddUpdate)

	action, err := protocol.Deserialize(requestTx.TxOut[requestOutputIndex].LockingScript,
		a.Config().IsTest)
	if err != nil {
		return errors.Wrap(err, "deserialize")
	}

	contractLockingScript := a.LockingScript()
	contractHash := state.CalculateContractHash(contractLockingScript)

	var contractFee uint64
	if contractFeeOutputIndex != -1 && contractFeeOutputIndex < len(responseTx.TxOut) {
		contractFee = responseTx.TxOut[contractFeeOutputIndex].Value
	}

	var instrumentUpdates []*statistics.Update
	contractUpdate := &statistics.Update{
		Time:         now,
		ContractHash: contractHash,
		Code:         action.Code(),
		WasRejected:  wasRejected,
		ContractFees: contractFee,
	}

	if !wasRejected {
		for _, transferFee := range transferFees {
			if transferFee == nil {
				continue
			}
			contractUpdate.TransferFees = transferFee.Value
		}
	}

	switch act := action.(type) {
	case *actions.ContractOffer:

	case *actions.ContractAmendment:

	case *actions.BodyOfAgreementOffer:

	case *actions.BodyOfAgreementFormation:

	case *actions.Order:

	case *actions.Proposal:

	case *actions.BallotCounted:
		contractUpdate.InputCount += 1

	case *actions.InstrumentDefinition:
		if !wasRejected {
			isTest := a.Config().IsTest
			for _, txout := range responseTx.TxOut {
				responseAction, err := protocol.Deserialize(txout.LockingScript, isTest)
				if err != nil {
					continue
				}

				if creation, ok := responseAction.(*actions.InstrumentCreation); ok {
					var instrumentCode state.InstrumentCode
					copy(instrumentCode[:], creation.InstrumentCode)

					instrumentUpdate := &statistics.Update{
						Time:           now,
						ContractHash:   contractHash,
						InstrumentCode: &instrumentCode,
						Code:           action.Code(),
						WasRejected:    wasRejected,
						ContractFees:   contractFee,
					}
					instrumentUpdates = append(instrumentUpdates, instrumentUpdate)

					break
				}
			}
		}

	case *actions.InstrumentModification:
		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], act.InstrumentCode)

		instrumentUpdate := &statistics.Update{
			Time:           now,
			ContractHash:   contractHash,
			InstrumentCode: &instrumentCode,
			Code:           action.Code(),
			WasRejected:    wasRejected,
			ContractFees:   contractFee,
		}
		instrumentUpdates = append(instrumentUpdates, instrumentUpdate)

	case *actions.Transfer:
		for instrumentIndex, instrumentTransfer := range act.Instruments {
			if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
				continue
			}

			if instrumentTransfer.ContractIndex >= uint32(len(requestTx.TxOut)) {
				continue
			}

			outputLockingScript := requestTx.TxOut[instrumentTransfer.ContractIndex].LockingScript
			if !outputLockingScript.Equal(contractLockingScript) {
				continue // not this contract
			}

			var instrumentCode state.InstrumentCode
			copy(instrumentCode[:], instrumentTransfer.InstrumentCode)

			instrumentUpdate := &statistics.Update{
				Time:           now,
				ContractHash:   contractHash,
				InstrumentCode: &instrumentCode,
				Code:           action.Code(),
				WasRejected:    wasRejected,
				ContractFees:   contractFee,
			}
			instrumentUpdates = append(instrumentUpdates, instrumentUpdate)

			if !wasRejected && len(transferFees) > instrumentIndex &&
				transferFees[instrumentIndex] != nil {
				instrumentUpdate.TransferFees = transferFees[instrumentIndex].Value
			}

			inputCount := uint64(len(instrumentTransfer.InstrumentSenders))
			outputCount := uint64(len(instrumentTransfer.InstrumentReceivers))

			contractUpdate.InputCount += inputCount
			instrumentUpdate.InputCount += inputCount
			contractUpdate.OutputCount += outputCount
			instrumentUpdate.OutputCount += outputCount
		}
	}

	if err := updateStats(ctx, contractUpdate); err != nil {
		return errors.Wrap(err, "add contract update")
	}

	for _, update := range instrumentUpdates {
		if err := updateStats(ctx, update); err != nil {
			return errors.Wrap(err, "add instrument update")
		}
	}

	return nil
}
