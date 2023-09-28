package funding

import (
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"
	"github.com/tokenized/txbuilder/fees"

	"github.com/pkg/errors"
)

const (
	FlatPayloadSize = 21 + // envelope header OP_0 OP_RETURN 445 OP_1 0x746573742e544b4e OP_3 0 "T2"
		8 + // timestamp
		0 // extra buffer
	FlatContractSize = txbuilder.P2PKHOutputSize + // standard P2PKH fee output
		txbuilder.MaximumP2PKHInputSize // response P2PKH input from contract agent
	FlatContractInputOutputSize = txbuilder.P2PKHOutputSize + // standard P2PKH output
		txbuilder.MaximumP2PKHInputSize // P2PKH input
	FlatInstrumentSize               = 30  // instrument type, code, and count of senders and receivers
	FlatParticipantSize              = 10  // 2 byte index, max 8 byte quantity
	FlatSettlementRequestInitialSize = 8 + // timestamp
		33 + // transfer txid
		txbuilder.P2PKHOutputScriptSize + // fee locking script
		8 + // fee value
		8 // message payload size
	FlatSettlementRequestSize = txbuilder.P2PKHOutputScriptSize + // fee locking script
		8 // fee value
	FlatSignatureRequestInitialSize = 8 + // timestamp
		8 // message payload size
)

var (
	ErrMissingAction = errors.New("Missing Action Payload")
	ErrMissingFee    = errors.New("Missing Fee")
	ErrMalformed     = errors.New("Malformed")
)

type ContractFee struct {
	LockingScript  bitcoin.Script // smart contract agent locking script
	ActionFee      uint64         // satoshis per action fee
	ParticipantFee uint64         // satoshis per participant fee
}

type OutputFunding struct {
	Index            int
	ActionFee        uint64
	InstrumentCount  int
	ParticipantCount int
	ParticipantsFee  uint64
	DustOutputsSize  int
	Dust             uint64
	ResponseFee      uint64
}

func (of OutputFunding) CalculateResponseFee(feeRate float64) uint64 {
	return fees.EstimateFeeValue(FlatContractSize+
		(FlatInstrumentSize*of.InstrumentCount)+
		(FlatParticipantSize*of.ParticipantCount)+
		of.DustOutputsSize, feeRate)
}

func (of OutputFunding) Value() uint64 {
	return of.ActionFee + of.ParticipantsFee + of.ResponseFee + of.Dust
}

type ResponseFunding struct {
	FeeRate   float64
	Contracts []*OutputFunding
	Boomerang uint64
}

// Calculate calculates the outputs tx fee based on the accumulated outputs size and fee rate. Also
// calculates the boomerang fee.
func (rf *ResponseFunding) calculate() {
	if len(rf.Contracts) == 0 {
		return
	}

	// Calculate response tx funding to give to master (first) contract agent.
	masterContractFunding := rf.Contracts[0]
	masterContractFunding.ResponseFee += fees.EstimateFeeValue(FlatPayloadSize, rf.FeeRate)

	boomerang := uint64(0)
	for i, contractFunding := range rf.Contracts {
		masterContractFunding.ResponseFee += contractFunding.CalculateResponseFee(rf.FeeRate)

		if i != 0 {
			rf.Boomerang += boomerang

			// The next settlement request adds to the previous data. Contract input and output
			// are already figured, so just increase the payload size.
			boomerang += fees.EstimateFeeValue(FlatSettlementRequestSize+
				(contractFunding.InstrumentCount*FlatInstrumentSize)+
				(contractFunding.ParticipantCount*FlatParticipantSize), rf.FeeRate)
		} else {
			// Contract input, contract output, and payload. No dust outputs.
			boomerang = fees.EstimateFeeValue(FlatContractInputOutputSize+FlatPayloadSize+
				FlatSettlementRequestInitialSize+
				(FlatInstrumentSize*contractFunding.InstrumentCount)+
				(FlatParticipantSize*contractFunding.ParticipantCount), rf.FeeRate)
		}
	}

	if rf.Boomerang > 0 {
		boomerang += fees.EstimateFeeValue(FlatSignatureRequestInitialSize, rf.FeeRate) +
			masterContractFunding.ResponseFee

		// Calculate the signature request rounds.
		for i := len(rf.Contracts) - 1; i > 0; i-- {
			rf.Boomerang += boomerang
		}
	}
}

func (rf ResponseFunding) EstimatedPayloadSize() int {
	result := FlatPayloadSize
	for _, contractFunding := range rf.Contracts {
		result += 8 + FlatInstrumentSize +
			(contractFunding.InstrumentCount * FlatInstrumentSize) +
			(contractFunding.ParticipantCount * FlatParticipantSize)
	}
	return result
}

// Charge flat fee per "participant" plus the tx mining fee for each participant's output based on
// the tx mining fee rate.

// Provide just one tx funding amount to give to the first (master) contract. And also funding for
// participant action fees. Somehow those need to get used to cover tx funding but retain the rest.

// The main variable that the requestor has control over is the size and number of the output
// scripts that will be in the settlement tx. The corresponding payload is just indexes and
// quantities which don't vary much. Indexes will pretty much always be 1 byte unless there are a
// lot of outputs. Quantities are 1 to 8 bytes depending on value. We would still need a non-zero
// participant fee because as the number of participants grows the settlement payload will grow. We
// can maybe use a flat 8 byte fee to account for the settlement data per participant. So we use the
// 8 bytes plus size of the output based on the participant locking script size.

// The other parts of the settlement transaction are the smart contract agent inputs, the full
// payload, and the fee output.

func TransferResponseFunding(tx expanded_tx.TransactionWithOutputs, contractFees []*ContractFee,
	feeRate, dustFeeRate float64, isTest bool) (*ResponseFunding, error) {

	// Find transfer
	var transfer *actions.Transfer
	outputCount := tx.OutputCount()
	for i := 0; i < outputCount; i++ {
		output := tx.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			continue
		}

		if t, ok := action.(*actions.Transfer); ok {
			transfer = t
			break
		}
	}

	if transfer == nil {
		return nil, ErrMissingAction
	}

	result := &ResponseFunding{
		FeeRate: feeRate,
	}
	for instrumentIndex, instrumentTransfer := range transfer.Instruments {
		if int(instrumentTransfer.ContractIndex) >= outputCount {
			return nil, errors.Wrapf(ErrMalformed,
				"Contract index out of range %d >= %d : instrument %d",
				int(instrumentTransfer.ContractIndex), outputCount, instrumentIndex)
		}
		output := tx.Output(int(instrumentTransfer.ContractIndex))

		var contractFee *ContractFee
		for _, contract := range contractFees {
			if contract.LockingScript.Equal(output.LockingScript) {
				contractFee = contract
				break
			}
		}

		if contractFee == nil {
			return nil, errors.Wrapf(ErrMissingFee, "contract index %d",
				instrumentTransfer.ContractIndex)
		}

		var contractFunding *OutputFunding
		for _, contract := range result.Contracts {
			if contract.Index == int(instrumentTransfer.ContractIndex) {
				contractFunding = contract
				contractFunding.InstrumentCount++
				break
			}
		}

		if contractFunding == nil {
			contractFunding = &OutputFunding{
				Index:           int(instrumentTransfer.ContractIndex),
				ActionFee:       contractFee.ActionFee,
				InstrumentCount: 1,
			}
			result.Contracts = append(result.Contracts, contractFunding)
		}

		for senderIndex, sender := range instrumentTransfer.InstrumentSenders {
			output, err := tx.InputOutput(int(sender.Index))
			if err != nil {
				return nil, errors.Wrap(ErrMalformed, errors.Wrapf(err,
					"input output: instrument %d, sender %d", instrumentIndex, senderIndex).Error())
			}

			contractFunding.DustOutputsSize += fees.OutputSize(output.LockingScript)
			contractFunding.Dust += fees.DustLimitForLockingScript(output.LockingScript,
				dustFeeRate)
			contractFunding.ParticipantsFee += contractFee.ParticipantFee
			contractFunding.ParticipantCount++
		}

		for receiverIndex, receiver := range instrumentTransfer.InstrumentReceivers {
			ra, err := bitcoin.DecodeRawAddress(receiver.Address)
			if err != nil {
				return nil, errors.Wrap(ErrMalformed, errors.Wrapf(err,
					"address: instrument %d, receiver %d", instrumentIndex, receiverIndex).Error())
			}

			ls, err := ra.LockingScript()
			if err != nil {
				return nil, errors.Wrap(ErrMalformed, errors.Wrapf(err,
					"locking script: instrument %d, receiver %d", instrumentIndex,
					receiverIndex).Error())
			}

			contractFunding.DustOutputsSize += fees.OutputSize(ls)
			contractFunding.Dust += fees.DustLimitForLockingScript(ls, dustFeeRate)
			contractFunding.ParticipantsFee += contractFee.ParticipantFee
			contractFunding.ParticipantCount++
		}
	}

	// Calculate output tx fees
	result.calculate()

	return result, nil
}
