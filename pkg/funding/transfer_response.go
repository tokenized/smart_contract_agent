package funding

import (
	"bytes"

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
	FlatContractSize = 16 + // version, locktime, input count, output count
		txbuilder.P2PKHOutputSize + // standard P2PKH fee output
		txbuilder.MaximumP2PKHInputSize // response P2PKH input from contract agent

	FlatEnvelopeSize = 21 + // envelope header OP_0 OP_RETURN 445 OP_1 0x746573742e544b4e OP_3 0 "T2"
		8 + // timestamp
		8 // output value for payload output

	FlatInputBaseSize = 44 // outpoint, sequence, and encoded script size

	FlatInstrumentSize      = 30 // instrument type, code, and count of senders and receivers
	FlatSettlementEntrySize = 10 // settlement entry, 2 byte index, max 8 byte quantity
)

var (
	ErrInvalid            = errors.New("Invalid")
	ErrMissingInformation = errors.New("Missing Information")
)

type FeeData struct {
	Index        uint32
	Value        uint64
	responseSize int    // size of response used to calculate response tx mining fee
	payloadSize  int    // size of response used to calculate response tx mining fee
	dust         uint64 // dust for settlement participant outputs
	fee          uint64 // action and transfer fees
}

type ContractData struct {
	LockingScript bitcoin.Script
	ActionFee     uint64
	Instruments   Instruments
}

type InstrumentData struct {
	Code                         []byte
	TransferFee                  uint64
	TransferFeeLockingScriptSize int
	TransferFeeInCurrentToken    bool // The transfer fee is paid in the token being transferred.
}

type Contracts []*ContractData

func (cs Contracts) Find(lockingScript bitcoin.Script) *ContractData {
	for _, contract := range cs {
		if contract.LockingScript.Equal(lockingScript) {
			return contract
		}
	}

	return nil
}

type Instruments []*InstrumentData

func (is Instruments) Find(code []byte) *InstrumentData {
	for _, instrument := range is {
		if bytes.Equal(instrument.Code, code) {
			return instrument
		}
	}

	return nil
}

func (f *FeeData) Finalize(isMaster bool, miningFeeRate float64) {
	f.Value = f.fee
	if isMaster {
		// Only the master contract funds the response tx.
		responseFee := fees.EstimateFeeValue(f.responseSize+f.payloadSize, miningFeeRate)
		f.Value += responseFee

		// Only the master contract's response contains the dust outputs
		f.Value += f.dust
	}
}

func UpdateTransferResponseFee(transferTx *wire.MsgTx, inputLockingScriptSizes []int,
	miningFeeRate, dustFeeRate float64, contracts []*ContractData, isTest bool) error {

	feeData, boomerang, err := CalculateTransferResponseFee(transferTx, inputLockingScriptSizes,
		miningFeeRate, dustFeeRate, contracts, isTest)
	if err != nil {
		return err
	}

	return UpdateTransaction(transferTx, feeData, boomerang)
}

func UpdateTransaction(transferTx *wire.MsgTx, feeData []*FeeData, boomerang uint64) error {
	for _, fee := range feeData {
		transferTx.TxOut[fee.Index].Value = fee.Value
	}

	if boomerang > 0 {
		// Find and update boomerang output.
		outputCount := uint32(len(transferTx.TxOut))
		masterFee := feeData[0]
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

func CalculateTransferResponseFee(transferTx *wire.MsgTx, inputLockingScriptSizes []int,
	miningFeeRate, dustFeeRate float64, contracts Contracts,
	isTest bool) ([]*FeeData, uint64, error) {

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

	var result []*FeeData

	// Find first contract and assign as "master" that pays the mining fee for the settlement tx.
	var masterFeeData *FeeData
	for _, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			continue
		}

		lockingScript := transferTx.TxOut[instrumentTransfer.ContractIndex].LockingScript
		contract := contracts.Find(lockingScript)
		if contract == nil {
			return nil, 0, errors.Wrapf(ErrMissingInformation, "contract: %s", lockingScript)
		}

		masterFeeData = &FeeData{
			Index:        instrumentTransfer.ContractIndex,
			responseSize: FlatContractSize,
			payloadSize:  FlatEnvelopeSize,
			dust:         0,
			fee:          contract.ActionFee,
		}
		result = append(result, masterFeeData)
		break
	}

	if masterFeeData == nil {
		return nil, 0, errors.Wrap(ErrInvalid, "no contracts found")
	}

	for _, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			// Account for bitcoin agent transfer inputs in master response tx. There should be one
			// input per receiver because the input can only be unlocked when paid directly to the
			// specific receiver address.
			for _, receiver := range instrumentTransfer.InstrumentReceivers {
				unlockingScriptSize := agent_bitcoin_transfer.ApproveUnlockingSize(txbuilder.MaximumP2PKHSigScriptSize)
				masterFeeData.responseSize += FlatInputBaseSize + unlockingScriptSize

				// Add the receiver bitcoin output size to the settlement tx.
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
				masterFeeData.responseSize += outputSize

				// Dust isn't necessary for this output as it will contain the bitcoin being
				// transferred.
			}

			continue
		}

		// Find contract fee data.
		var currentFeeData *FeeData
		for _, feeData := range result {
			if feeData.Index == instrumentTransfer.ContractIndex {
				currentFeeData = feeData
				break
			}
		}

		lockingScript := transferTx.TxOut[instrumentTransfer.ContractIndex].LockingScript
		contract := contracts.Find(lockingScript)
		if contract == nil {
			return nil, 0, errors.Wrapf(ErrMissingInformation, "contract: %s", lockingScript)
		}

		if currentFeeData == nil { // a new contract is involved
			currentFeeData = &FeeData{
				Index:        instrumentTransfer.ContractIndex,
				responseSize: FlatContractSize,
				dust:         0,
				fee:          contract.ActionFee,
			}
			result = append(result, currentFeeData)
		}

		// Add instrument overhead.
		currentFeeData.payloadSize += FlatInstrumentSize

		instrument := contract.Instruments.Find(instrumentTransfer.InstrumentCode)
		if instrument == nil {
			return nil, 0, errors.Wrapf(ErrMissingInformation, "instrument: %x",
				instrumentTransfer.InstrumentCode)
		}

		if instrument.TransferFee > 0 && instrument.TransferFeeLockingScriptSize > 0 {
			if !instrument.TransferFeeInCurrentToken {
				currentFeeData.fee += instrument.TransferFee
			} else {
				// Dust for settlement output of transfer fee locking script
				dust := fees.DustLimitForLockingScriptSize(instrument.TransferFeeLockingScriptSize,
					dustFeeRate)
				currentFeeData.dust += dust
			}

			outputSize := fees.OutputSizeForLockingScriptSize(instrument.TransferFeeLockingScriptSize)
			currentFeeData.responseSize += outputSize
		}

		// Add settlement data and dust outputs for each participant (senders and receivers).
		for _, sender := range instrumentTransfer.InstrumentSenders {
			outputSize := fees.OutputSizeForLockingScriptSize(inputLockingScriptSizes[sender.Index])
			currentFeeData.responseSize += outputSize
			currentFeeData.dust += fees.DustLimit(outputSize, dustFeeRate)
			currentFeeData.payloadSize += FlatSettlementEntrySize
		}

		for _, receiver := range instrumentTransfer.InstrumentReceivers {
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
			currentFeeData.responseSize += outputSize
			currentFeeData.dust += fees.DustLimit(outputSize, dustFeeRate)
			currentFeeData.payloadSize += FlatSettlementEntrySize
		}
	}

	contractCount := len(result)
	boomerangFees := uint64(0)
	boomerangSize := 0
	if contractCount > 1 {
		// Add the response size of all other contract's responses into the master's since it must
		// pay the mining fee for the entire settlement tx.
		for _, feeData := range result {
			if feeData == masterFeeData {
				continue
			}

			// Add other contract response size to master response size.
			masterFeeData.responseSize += feeData.responseSize
			masterFeeData.dust += feeData.dust
			masterFeeData.payloadSize += feeData.payloadSize
		}

		// Calculate boomerang message tx fees.
		boomerangOutgoingPayloadSize := 0
		for _, feeData := range result[:contractCount-1] { // the last contract doesn't send this
			// Boomerang outgoing settlement request messages accumulating settlement data.
			boomerangOutgoingPayloadSize += feeData.payloadSize

			// Sender contract input and receiver contract output plus accumulated settlement
			// payload.
			boomerangSize += FlatContractSize + FlatEnvelopeSize
			boomerangSize += boomerangOutgoingPayloadSize

			// Boomerang returning signature request messages containing full response tx and adding
			// signatures.
			// Sender contract input and receiver contract output plus accumulated response tx.
			boomerangSize += FlatContractSize + FlatEnvelopeSize
			boomerangSize += masterFeeData.responseSize + masterFeeData.payloadSize

			boomerangFees += fees.EstimateFeeValue(boomerangSize, miningFeeRate)
		}

		for i := contractCount - 1; i >= 1; i-- { // the last contract doesn't send this
			// Boomerang returning signature request messages containing full response tx and adding
			// signatures.
			// Sender contract input and receiver contract output plus accumulated response tx.
			boomerangSize += FlatContractSize + FlatEnvelopeSize
			boomerangSize += masterFeeData.responseSize + masterFeeData.payloadSize

			boomerangFees += fees.EstimateFeeValue(boomerangSize, miningFeeRate)
		}
	}

	// Calculate each contract's output value based on the response sizes.
	for _, feeData := range result {
		feeData.Finalize(feeData == masterFeeData, miningFeeRate)
	}

	return result, boomerangFees, nil
}
