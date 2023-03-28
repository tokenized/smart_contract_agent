package commands

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/tokenized/channels/contract_operator"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var CreateContractOffer = &cobra.Command{
	Use:   "create_contract_offer operator_peer_channel response_write_peer_channel response_read_token client_key admin_key outpoint outpoint_value contract_script_hex contract_offer_json_filename",
	Short: "Sends a request to the smart contract agent operator to sign a contract offer transaction",
	Args:  cobra.ExactArgs(9),
	RunE: func(c *cobra.Command, args []string) error {
		ctx := logger.ContextWithLogger(context.Background(), true, true, "")
		peerChannelsFactory := peer_channels.NewFactory()

		operatorPeerChannel, err := peer_channels.ParseChannel(args[0])
		if err != nil {
			return errors.Wrap(err, "agent peer channel")
		}

		responsePeerChannel, err := peer_channels.ParseChannel(args[1])
		if err != nil {
			return errors.Wrap(err, "response peer channel")
		}

		responseReadToken := args[2]

		clientKey, err := bitcoin.KeyFromStr(args[3])
		if err != nil {
			return errors.Wrap(err, "client_key")
		}

		adminKey, err := bitcoin.KeyFromStr(args[4])
		if err != nil {
			return errors.Wrap(err, "admin key")
		}

		outpoint, err := wire.OutPointFromStr(args[5])
		if err != nil {
			return errors.Wrap(err, "outpoint")
		}

		outpointValue, err := strconv.ParseUint(args[6], 10, 64)
		if err != nil {
			return errors.Wrap(err, "outpoint value")
		}

		b, err := hex.DecodeString(args[7])
		if err != nil {
			return errors.Wrap(err, "contract_script_hex")
		}
		contractLockingScript := bitcoin.Script(b)

		if !contractLockingScript.IsP2PKH() {
			// Other script types are supported, but this is a safety error in case the hex provided
			// was malformed.
			return errors.Wrap(errors.New("Script is not P2PKH"), "contract_script_hex")
		}

		jsFile, err := os.Open(args[8])
		if err != nil {
			return errors.Wrap(err, "open file")
		}
		defer jsFile.Close()

		jsFileSize, err := jsFile.Seek(0, 2)
		if err != nil {
			return errors.Wrap(err, "seek file end")
		}

		if _, err := jsFile.Seek(0, 0); err != nil {
			return errors.Wrap(err, "seek file begin")
		}

		js := make([]byte, jsFileSize)
		if readSize, err := jsFile.Read(js); err != nil {
			return errors.Wrap(err, "read file")
		} else if readSize != int(jsFileSize) {
			return fmt.Errorf("Failed to read full file: read %d, size %d", readSize, jsFileSize)
		}

		mockOperatorKey, _ := bitcoin.GenerateKey(bitcoin.MainNet)
		mockOperatorLockingScript, _ := mockOperatorKey.LockingScript()

		etx, err := createContractOfferTx(ctx, adminKey, outpoint, outpointValue,
			contractLockingScript, mockOperatorLockingScript, js)
		if err != nil {
			return errors.Wrap(err, "create contract offer tx")
		}

		id := uuid.New()

		msg := &contract_operator.SignTx{
			Tx: etx,
		}

		if err = sendOperatorRequest(ctx, peerChannelsFactory, *operatorPeerChannel,
			*responsePeerChannel, clientKey, id, msg); err != nil {
			return errors.Wrap(err, "request")
		}

		response, err := waitForOperatorResponse(ctx, peerChannelsFactory, *responsePeerChannel,
			responseReadToken, id)
		if err != nil {
			return errors.Wrap(err, "response")
		}

		signedTx, ok := response.Msg.(*contract_operator.SignedTx)
		if !ok {

		}

		feeRate, err := FeeRate()
		if err != nil {
			return errors.Wrap(err, "fee rate")
		}

		dustFeeRate, err := DustFeeRate()
		if err != nil {
			return errors.Wrap(err, "dust fee rate")
		}

		txb, err := txbuilder.NewTxBuilderFromTransactionWithOutputs(float32(feeRate),
			float32(dustFeeRate), signedTx.Tx)
		if err != nil {
			return errors.Wrap(err, "tx builder")
		}

		if _, err := txb.SignOnly([]bitcoin.Key{adminKey}); err != nil {
			return errors.Wrap(err, "sign tx")
		}

		signedTx.Tx.Tx = txb.MsgTx

		script, err := wrapExpandedTx(signedTx.Tx)
		if err != nil {
			return errors.Wrap(err, "wrap")
		}

		fmt.Printf("Expanded Tx: %s\n", signedTx.Tx)
		fmt.Printf("Expanded Tx Message Hex: %x\n", []byte(script))

		return nil
	},
}

func createContractOfferTx(ctx context.Context, adminKey bitcoin.Key, outpoint *wire.OutPoint,
	outpointValue uint64, contractLockingScript, operatorLockingScript bitcoin.Script,
	js []byte) (*expanded_tx.ExpandedTx, error) {

	adminLockingScript, err := adminKey.LockingScript()
	if err != nil {
		return nil, errors.Wrap(err, "admin key locking script")
	}

	contractOffer := &actions.ContractOffer{}
	if err := json.Unmarshal(js, contractOffer); err != nil {
		return nil, errors.Wrap(err, "unmarshal contract offer")
	}

	if err := contractOffer.Validate(); err != nil {
		return nil, errors.Wrap(err, "validate contract offer")
	}

	isTest, err := IsTest()
	if err != nil {
		return nil, errors.Wrap(err, "is test")
	}

	contractOfferScript, err := protocol.Serialize(contractOffer, isTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize contract offer")
	}

	feeRate, err := FeeRate()
	if err != nil {
		return nil, errors.Wrap(err, "fee rate")
	}

	dustFeeRate, err := DustFeeRate()
	if err != nil {
		return nil, errors.Wrap(err, "dust fee rate")
	}

	tx := txbuilder.NewTxBuilder(float32(feeRate), float32(dustFeeRate))

	if err := tx.AddInput(*outpoint, adminLockingScript, outpointValue); err != nil {
		return nil, errors.Wrap(err, "add input")
	}

	// Add temporary input so fee calculation will be correct after the operator adds their input.
	emptyOutPoint := wire.OutPoint{}
	temporaryInputIndex := len(tx.Inputs)
	if err := tx.AddInput(emptyOutPoint, operatorLockingScript, 1); err != nil {
		return nil, errors.Wrap(err, "add temporary input")
	}

	if err := tx.AddOutput(contractLockingScript, 0, false, true); err != nil {
		return nil, errors.Wrap(err, "add contract output")
	}

	if err := tx.AddOutput(contractOfferScript, 0, false, false); err != nil {
		return nil, errors.Wrap(err, "add contract offer output")
	}

	// Add temporary output so fee calculation will be correct after the operator adds their output.
	temporaryOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(operatorLockingScript, 1, false, true); err != nil {
		return nil, errors.Wrap(err, "add temporary output")
	}

	if err := tx.SetChangeLockingScript(adminLockingScript, ""); err != nil {
		return nil, errors.Wrap(err, "set change locking script")
	}

	// Estimate contract response funding.
	inputLockingScripts := []bitcoin.Script{
		adminLockingScript,
		operatorLockingScript,
	}
	responseFee, err := protocol.EstimatedContractOfferResponseTxFee(contractOffer,
		contractLockingScript, contractLockingScript, inputLockingScripts, feeRate, dustFeeRate,
		protocol.SamplePeerChannelSize, isTest)
	if err != nil {
		return nil, errors.Wrap(err, "estimate funding")
	}
	println("Estimated response tx fee:", responseFee)

	if err := tx.AddValueToOutput(0, responseFee+contractOffer.ContractFee); err != nil {
		return nil, errors.Wrap(err, "add funding value")
	}

	if err := tx.CalculateFee(); err != nil {
		return nil, errors.Wrap(err, "calculate fee")
	}

	if err := tx.RemoveInput(temporaryInputIndex); err != nil {
		return nil, errors.Wrap(err, "remove input")
	}

	if err := tx.RemoveOutput(temporaryOutputIndex); err != nil {
		return nil, errors.Wrap(err, "remove output")
	}

	etx := &expanded_tx.ExpandedTx{
		Tx: tx.MsgTx,
	}

	for i, txin := range tx.MsgTx.TxIn {
		if etx.Ancestors.GetTx(txin.PreviousOutPoint.Hash) != nil {
			continue
		}

		atx, err := getTx(ctx, txin.PreviousOutPoint.Hash)
		if err != nil {
			return nil, errors.Wrapf(err, "get input tx %d: %s", i, txin.PreviousOutPoint.Hash)
		}

		etx.Ancestors = append(etx.Ancestors, &expanded_tx.AncestorTx{
			Tx: atx,
		})
	}

	println("Expanded Tx:", etx.String())

	return etx, nil
}
