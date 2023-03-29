package operator_client

import (
	"bytes"
	"context"
	"math/rand"

	"github.com/tokenized/channels"
	"github.com/tokenized/channels/contract_operator"
	"github.com/tokenized/channels/peer_channels_listener"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/txbuilder"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type MockOperator struct {
	operatorKey bitcoin.Key

	clientPublicKey bitcoin.PublicKey
	channelID       string
	contractFee     uint64
	agentKeys       []bitcoin.Key

	peerChannelFactory *peer_channels.Factory
}

func NewMockOperator(peerChannelFactory *peer_channels.Factory, operatorKey bitcoin.Key,
	clientPublicKey bitcoin.PublicKey, channelID string, contractFee uint64) *MockOperator {

	return &MockOperator{
		operatorKey:        operatorKey,
		clientPublicKey:    clientPublicKey,
		channelID:          channelID,
		contractFee:        contractFee,
		peerChannelFactory: peerChannelFactory,
	}
}

func (o *MockOperator) HandleMessage(ctx context.Context, msg peer_channels.Message) error {
	if o.channelID != msg.ChannelID {
		return errors.Wrap(peer_channels_listener.MessageNotRelevent, "channel id")
	}

	request, err := UnwrapRequest(bitcoin.Script(msg.Payload))
	if err != nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.String("channel_id", msg.ChannelID),
		}, "Failed to unwrap operator request : %s", err)
		return nil
	}

	if request.ReplyTo == nil || request.ReplyTo.PeerChannel == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.String("channel_id", msg.ChannelID),
			logger.Uint64("sequence", msg.Sequence),
		}, "Operator request is missing ReplyTo peer channel")
		return nil
	}

	if request.ID == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.String("channel_id", msg.ChannelID),
			logger.Uint64("sequence", msg.Sequence),
		}, "Operator request is missing ID")
		return nil
	}

	if request.Signature == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.String("channel_id", msg.ChannelID),
			logger.Uint64("sequence", msg.Sequence),
		}, "Operator request is missing signature")
		return nil
	}

	request.Signature.SetPublicKey(&o.clientPublicKey)
	if err := request.Signature.Verify(); err != nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.String("channel_id", msg.ChannelID),
			logger.Uint64("sequence", msg.Sequence),
		}, "Operator request signature is invalid")
		return nil
	}

	switch m := request.Msg.(type) {
	case *contract_operator.CreateAgent:
		logger.InfoWithFields(ctx, []logger.Field{
			logger.String("channel_id", msg.ChannelID),
			logger.Uint64("sequence", msg.Sequence),
		}, "Operator creating agent")

		if err := o.createAgent(ctx, request.ReplyTo, *request.ID, m); err != nil {
			return errors.Wrap(err, "create agent")
		}

		return nil

	case *contract_operator.SignTx:
		logger.InfoWithFields(ctx, []logger.Field{
			logger.String("channel_id", msg.ChannelID),
			logger.Uint64("sequence", msg.Sequence),
		}, "Operator signing tx")

		if err := o.signTx(ctx, request.ReplyTo, *request.ID, m); err != nil {
			return errors.Wrap(err, "sign tx")
		}

		return nil

	default:
		logger.WarnWithFields(ctx, []logger.Field{
			logger.String("channel_id", msg.ChannelID),
			logger.Uint64("sequence", msg.Sequence),
		}, "Operator request is unknown type")
		return nil
	}
}

func (o *MockOperator) createAgent(ctx context.Context, replyTo *channels.ReplyTo, id uuid.UUID,
	request *contract_operator.CreateAgent) error {

	key, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	lockingScript, _ := key.LockingScript()
	o.agentKeys = append(o.agentKeys, key)

	masterKey, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	masterLockingScript, _ := masterKey.LockingScript()

	agent := &contract_operator.Agent{
		LockingScript:       lockingScript,
		ContractFee:         o.contractFee,
		MasterLockingScript: masterLockingScript,
		PeerChannel:         nil,
	}

	if err := o.sendResponse(ctx, replyTo, agent, id, &channels.Response{
		Status:         channels.StatusOK,
		CodeProtocolID: contract_operator.ProtocolID,
		Note:           "agent created",
	}, o.operatorKey); err != nil {
		return errors.Wrap(err, "send response")
	}

	return nil
}

func (o *MockOperator) signTx(ctx context.Context, replyTo *channels.ReplyTo, id uuid.UUID,
	request *contract_operator.SignTx) error {

	tx := request.Tx.Tx

	serviceAddress, err := o.operatorKey.RawAddress()
	if err != nil {
		return errors.Wrap(err, "address")
	}

	serviceLockingScript, err := serviceAddress.LockingScript()
	if err != nil {
		return errors.Wrap(err, "locking script")
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

	request.Tx.Ancestors = append(request.Tx.Ancestors, &expanded_tx.AncestorTx{
		Tx: fundingTx,
	})

	output := wire.NewTxOut(utxo.Value, serviceLockingScript)
	tx.AddTxOut(output)

	// Sign input based on current tx. Note: The client can only add signatures after this or they
	// will invalidate this signature.
	input.UnlockingScript, err = txbuilder.P2PKHUnlockingScript(o.operatorKey, tx, inputIndex,
		utxo.LockingScript, utxo.Value, txbuilder.SigHashAll+txbuilder.SigHashForkID,
		&txbuilder.SigHashCache{})
	if err != nil {
		return errors.Wrap(err, "sign")
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
		logger.Stringer("public_key", o.operatorKey.PublicKey()),
	}, "Signing contract agent input with key")

	signedTx := &contract_operator.SignedTx{
		Tx: request.Tx,
	}

	if err := o.sendResponse(ctx, replyTo, signedTx, id, &channels.Response{
		Status:         channels.StatusOK,
		CodeProtocolID: contract_operator.ProtocolID,
		Note:           "contract offer signed",
	}, o.operatorKey); err != nil {
		return errors.Wrap(err, "send response")
	}

	return nil
}

func (o *MockOperator) sendResponse(ctx context.Context, replyTo *channels.ReplyTo,
	msg channels.Writer, id uuid.UUID, response *channels.Response, key bitcoin.Key) error {

	peerChannelsClient, err := o.peerChannelFactory.NewClient(replyTo.PeerChannel.String())
	if err != nil {
		return errors.Wrap(err, "peer channels client")
	}

	payload, err := WrapResponse(msg, id, response, o.operatorKey)
	if err != nil {
		return errors.Wrap(err, "wrap")
	}

	if err := peerChannelsClient.WriteMessage(ctx, replyTo.PeerChannel.ChannelID,
		replyTo.PeerChannel.Token, peer_channels.ContentTypeBinary,
		bytes.NewReader(payload)); err != nil {
		return errors.Wrap(err, "write message")
	}

	return nil
}
