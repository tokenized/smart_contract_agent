package operator_client

import (
	"context"
	"math/rand"

	"github.com/tokenized/channels/contract_operator"
	"github.com/tokenized/channels/unlocking_data"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/txbuilder"

	"github.com/pkg/errors"
)

type MockClient struct {
	operatorKey        bitcoin.Key
	minimumContractFee uint64
	agentKeys          []bitcoin.Key
	agents             []*MockAgent
}

type MockAgent struct {
	AdminLockingScript  bitcoin.Script `json:"admin_locking_script"`
	LockingScript       bitcoin.Script `json:"locking_script"`
	MinimumContractFee  uint64         `json:"minimum_contract_fee"`
	FeeLockingScript    bitcoin.Script `json:"fee_locking_script"`
	MasterLockingScript bitcoin.Script `json:"master_locking_script"`
}

func NewMockClient(operatorKey bitcoin.Key, minimumContractFee uint64) (*MockClient, error) {
	return &MockClient{
		operatorKey:        operatorKey,
		minimumContractFee: minimumContractFee,
	}, nil
}

// RequestNewAgent requests a new smart contract agent be created.
func (c *MockClient) RequestNewAgent(ctx context.Context,
	adminLockingScript, feeLockingScript, masterLockingScript bitcoin.Script,
	minimumContractFee uint64) (*contract_operator.Agent, error) {

	key, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	lockingScript, _ := key.LockingScript()
	c.agentKeys = append(c.agentKeys, key)

	if len(masterLockingScript) == 0 {
		masterKey, _ := bitcoin.GenerateKey(bitcoin.MainNet)
		masterLockingScript, _ = masterKey.LockingScript()
	}

	if minimumContractFee < c.minimumContractFee {
		return nil, errors.New("Minimum contract fee too low")
	}

	c.agents = append(c.agents, &MockAgent{
		AdminLockingScript:  adminLockingScript,
		LockingScript:       lockingScript,
		MinimumContractFee:  minimumContractFee,
		FeeLockingScript:    feeLockingScript,
		MasterLockingScript: masterLockingScript,
	})

	result := &contract_operator.Agent{
		LockingScript:       lockingScript,
		MinimumContractFee:  minimumContractFee,
		MasterLockingScript: masterLockingScript,
		PeerChannel:         nil,
	}

	return result, nil
}

func (c *MockClient) GetAgents() []*MockAgent {
	return c.agents
}

func (c *MockClient) RequestSignedInput(ctx context.Context,
	etx *expanded_tx.ExpandedTx) (*expanded_tx.ExpandedTx, error) {

	println("RequestSignedInput")

	etxc := etx.Copy()
	etx = &etxc
	txb, err := txbuilder.NewTxBuilderFromTransactionWithOutputs(0.05, 0.0, etx)
	if err != nil {
		return nil, errors.Wrap(err, "tx builder")
	}

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

	if err := txb.InsertInput(1, utxo); err != nil {
		return nil, errors.Wrap(err, "insert input")
	}

	if err := txb.AddOutput(serviceLockingScript, utxo.Value, false, true); err != nil {
		return nil, errors.Wrap(err, "add output")
	}

	if err := txb.SignP2PKHInput(1, c.operatorKey, &txbuilder.SigHashCache{}); err != nil {
		return nil, errors.Wrap(err, "sign")
	}

	etx.Ancestors = append(etx.Ancestors, &expanded_tx.AncestorTx{
		Tx: fundingTx,
	})

	println("signed tx", unlocking_data.TxString(etx))

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("hash", utxo.Hash),
		logger.Stringer("unlocking_script", txb.MsgTx.TxIn[1].UnlockingScript),
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
