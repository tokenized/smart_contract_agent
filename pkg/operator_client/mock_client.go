package operator_client

import (
	"context"
	"math/rand"

	"github.com/tokenized/channels/contract_operator"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/txbuilder"

	"github.com/pkg/errors"
)

type MockClient struct {
	operatorKey bitcoin.Key
	contractFee uint64
	agentKeys   []bitcoin.Key
}

func NewMockClient(operatorKey bitcoin.Key, contractFee uint64) (*MockClient, error) {
	return &MockClient{
		operatorKey: operatorKey,
		contractFee: contractFee,
	}, nil
}

// RequestNewAgent requests a new smart contract agent be created.
func (c *MockClient) RequestNewAgent(ctx context.Context,
	adminLockingScript bitcoin.Script) (*contract_operator.Agent, error) {

	key, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	lockingScript, _ := key.LockingScript()
	c.agentKeys = append(c.agentKeys, key)

	masterKey, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	masterLockingScript, _ := masterKey.LockingScript()

	result := &contract_operator.Agent{
		LockingScript:       lockingScript,
		ContractFee:         c.contractFee,
		MasterLockingScript: masterLockingScript,
		PeerChannel:         nil,
	}

	return result, nil
}

func (c *MockClient) SignContractOffer(ctx context.Context,
	etx *expanded_tx.ExpandedTx) (*expanded_tx.ExpandedTx, error) {

	etxc := etx.Copy()
	etx = &etxc
	tx := etx.Tx

	serviceAddress, err := c.operatorKey.RawAddress()
	if err != nil {
		return nil, errors.Wrap(err, "address")
	}

	serviceLockingScript, err := serviceAddress.LockingScript()
	if err != nil {
		return nil, errors.Wrap(err, "locking script")
	}

	fundingTx := wire.NewMsgTx(1)

	randomInput := &wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Index: uint32(rand.Intn(10)),
		},
		Sequence: wire.MaxTxInSequenceNum,
	}
	rand.Read(randomInput.PreviousOutPoint.Hash[:])
	fundingTx.AddTxIn(randomInput)

	fundingTx.AddTxOut(wire.NewTxOut(txbuilder.DustLimitForLockingScript(serviceLockingScript, 0.0),
		serviceLockingScript))

	fundingTxHash := *fundingTx.TxHash()
	utxo := bitcoin.UTXO{
		Hash:          fundingTxHash,
		Index:         0,
		Value:         fundingTx.TxOut[0].Value,
		LockingScript: fundingTx.TxOut[0].LockingScript,
	}

	// Add dust input from service key and output back to service key.
	inputIndex := 1 // contract operator input must be immediately after admin input
	input := wire.NewTxIn(wire.NewOutPoint(&utxo.Hash, utxo.Index), nil)

	if len(tx.TxIn) > 1 {
		after := make([]*wire.TxIn, len(tx.TxIn)-1)
		copy(after, tx.TxIn[1:])
		tx.TxIn = append(append(tx.TxIn[:1], input), after...)
	} else {
		tx.TxIn = append(tx.TxIn, input)
	}

	etx.Ancestors = append(etx.Ancestors, &expanded_tx.AncestorTx{
		Tx: fundingTx,
	})

	output := wire.NewTxOut(utxo.Value, serviceLockingScript)
	tx.AddTxOut(output)

	// Sign input based on current tx. Note: The client can only add signatures after this or they
	// will invalidate this signature.
	input.UnlockingScript, err = txbuilder.P2PKHUnlockingScript(c.operatorKey, tx, inputIndex,
		utxo.LockingScript, utxo.Value, txbuilder.SigHashAll+txbuilder.SigHashForkID,
		&txbuilder.SigHashCache{})
	if err != nil {
		return nil, errors.Wrap(err, "sign")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("hash", utxo.Hash),
		logger.Stringer("unlocking_script", input.UnlockingScript),
	}, "Added contract agent input")

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("value", utxo.Value),
		logger.Stringer("script", serviceLockingScript),
	}, "Adding contract agent output")

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("public_key", c.operatorKey.PublicKey()),
	}, "Signing contract agent input with key")

	return etx, nil
}
