package funding

import (
	"github.com/tokenized/bitcoin_interpreter/agent_bitcoin_transfer"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"
	"github.com/tokenized/txbuilder/fees"

	"github.com/pkg/errors"
)

const (
	FlatParticipantSize = 10 // size used to charge contract fees per participant
)

type RateFeeData struct {
	Index        uint32
	Value        uint64
	feeSize      int    // size based only on cost to contract agent used to charge fee
	responseSize int    // size of response used to calculate response tx mining fee
	payloadSize  int    // size of response used to calculate response tx mining fee
	dust         uint64 // dust for settlement participant outputs
}

// OutputDetails is used to estimate the size of outputs like fee outputs for contract responses.
type OutputDetails struct {
	Value             uint64
	LockingScriptSize int
}

func (f *RateFeeData) Finalize(isMaster bool, contractFeeRate, miningFeeRate float64) {
	f.Value = uint64(float64(f.feeSize) * contractFeeRate)
	if isMaster {
		// Only the master contract funds the response tx.
		responseFee := fees.EstimateFeeValue(f.responseSize+f.payloadSize, miningFeeRate)
		f.Value += responseFee

		// Only the master contract's response contains the dust outputs
		f.Value += f.dust
	}
}

func (f *RateFeeData) ContractFee(contractFeeRate float64) uint64 {
	return uint64(float64(f.feeSize) * contractFeeRate)
}

func UpdateTransferResponseRateFee(transferTx *wire.MsgTx, inputLockingScriptSizes []int,
	miningFeeRate, dustFeeRate float64, contractFeeRates []float64, transferFees []OutputDetails,
	isTest bool) error {

	contractFees, boomerang, err := CalculateTransferResponseRateFee(transferTx, inputLockingScriptSizes,
		miningFeeRate, dustFeeRate, contractFeeRates, transferFees, isTest)
	if err != nil {
		return err
	}

	return UpdateTransactionRateFee(transferTx, contractFees, boomerang)
}

func UpdateTransactionRateFee(transferTx *wire.MsgTx, contractFees []*RateFeeData,
	boomerang uint64) error {

	for _, fee := range contractFees {
		transferTx.TxOut[fee.Index].Value = fee.Value
	}

	if boomerang > 0 {
		// Find and update boomerang output.
		outputCount := uint32(len(transferTx.TxOut))
		masterFee := contractFees[0]
		masterLockingScript := transferTx.TxOut[masterFee.Index].LockingScript
		found := false
		for i := masterFee.Index + 1; i < outputCount; i++ {
			if masterLockingScript.Equal(transferTx.TxOut[i].LockingScript) {
				transferTx.TxOut[i].Value = boomerang
				found = true
				break
			}
		}

		if !found {
			transferTx.AddTxOut(wire.NewTxOut(boomerang, masterLockingScript))
		}
	}

	return nil
}

func CalculateTransferResponseRateFee(transferTx *wire.MsgTx, inputLockingScriptSizes []int,
	miningFeeRate, dustFeeRate float64, contractFeeRates []float64, transferFees []OutputDetails,
	isTest bool) ([]*RateFeeData, uint64, error) {

	// Find the transfer payload in the transaction.
	var transfer *actions.Transfer
	for _, txout := range transferTx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, isTest)
		if err != nil {
			continue
		}

		if t, ok := action.(*actions.Transfer); ok {
			transfer = t
			break
		}
	}

	if transfer == nil {
		return nil, 0, errors.Wrap(ErrInvalid, "Transfer payload output not found")
	}

	var result []*RateFeeData

	// Find first contract and assign as "master" that creates the final response tx.
	var masterRateFeeData *RateFeeData
	for _, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			continue
		}

		masterRateFeeData = &RateFeeData{
			Index:        instrumentTransfer.ContractIndex,
			feeSize:      FlatContractSize,
			responseSize: FlatContractSize,
			payloadSize:  FlatEnvelopeSize,
			dust:         0,
		}
		result = append(result, masterRateFeeData)
		break
	}

	if masterRateFeeData == nil {
		return nil, 0, errors.Wrap(ErrInvalid, "no contracts found")
	}

	for _, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			// Account for bitcoin agent transfer inputs in master response tx.
			for range instrumentTransfer.InstrumentReceivers {
				unlockingScriptSize := agent_bitcoin_transfer.ApproveUnlockingSize(txbuilder.MaximumP2PKHSigScriptSize)
				masterRateFeeData.responseSize += FlatInputBaseSize + unlockingScriptSize
			}

			continue
		}

		var currentRateFeeData *RateFeeData
		for _, feeData := range result {
			if feeData.Index == instrumentTransfer.ContractIndex {
				currentRateFeeData = feeData
				break
			}
		}

		if currentRateFeeData == nil {
			currentRateFeeData = &RateFeeData{
				Index:        instrumentTransfer.ContractIndex,
				feeSize:      FlatContractSize,
				responseSize: FlatContractSize,
				dust:         0,
			}
			result = append(result, currentRateFeeData)
		}

		currentRateFeeData.feeSize += FlatInstrumentSize
		currentRateFeeData.payloadSize += FlatInstrumentSize

		for _, sender := range instrumentTransfer.InstrumentSenders {
			currentRateFeeData.feeSize += FlatParticipantSize

			outputSize := fees.OutputSizeForLockingScriptSize(inputLockingScriptSizes[sender.Index])
			currentRateFeeData.responseSize += outputSize
			currentRateFeeData.dust += fees.DustLimit(outputSize, float64(dustFeeRate))
			currentRateFeeData.payloadSize += FlatSettlementEntrySize
		}

		for _, receiver := range instrumentTransfer.InstrumentReceivers {
			currentRateFeeData.feeSize += FlatParticipantSize

			ra, err := bitcoin.DecodeRawAddress(receiver.Address)
			if err != nil {
				return nil, 0, errors.Wrap(ErrInvalid, errors.Wrap(err, "receiver address").Error())
			}

			lockingScript, err := ra.LockingScript()
			if err != nil {
				return nil, 0, errors.Wrap(ErrInvalid,
					errors.Wrap(err, "receiver locking script").Error())
			}

			outputSize := fees.OutputSizeForLockingScriptSize(len(lockingScript))
			currentRateFeeData.responseSize += outputSize
			currentRateFeeData.dust += fees.DustLimit(outputSize, float64(dustFeeRate))
			currentRateFeeData.payloadSize += FlatSettlementEntrySize
		}
	}

	// Add the size of all other contract's responses into the master's since it must create the
	// full response.
	for _, feeData := range result {
		if feeData == masterRateFeeData {
			continue
		}

		// Add other contract response size to master response size.
		masterRateFeeData.responseSize += feeData.responseSize
		masterRateFeeData.dust += feeData.dust
		masterRateFeeData.payloadSize += feeData.payloadSize
	}

	contractCount := len(result)
	boomerangSize := 0
	if contractCount > 1 {
		// Calculate boomerang message tx fees.
		boomerangOutgoingPayloadSize := 0
		for _, feeData := range result[:contractCount-1] { // the last contract doesn't send this
			// Boomerang outgoing messages accumulating settlement data.
			boomerangOutgoingPayloadSize += feeData.payloadSize

			// Sender contract input and receiver contract output plus accumulated settlement
			// payload.
			boomerangSize += FlatContractSize + FlatEnvelopeSize
			boomerangSize += boomerangOutgoingPayloadSize
		}

		// Size of response tx
		masterResponseSize := masterRateFeeData.responseSize

		for i := contractCount - 1; i >= 1; i-- { // the first contract doesn't send this
			// Boomerang returning messages containing full response tx and adding signatures.
			// Sender contract input and receiver contract output plus accumulated response tx.
			boomerangSize += FlatContractSize + FlatEnvelopeSize
			boomerangSize += masterResponseSize
		}
	}

	// Calculate each contract's output value based on the response sizes.
	for i, feeData := range result {
		feeData.Finalize(feeData == masterRateFeeData, contractFeeRates[i], miningFeeRate)
	}

	return result, fees.EstimateFeeValue(boomerangSize, miningFeeRate), nil
}
