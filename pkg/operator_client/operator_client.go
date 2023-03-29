package operator_client

import (
	"bytes"
	"context"
	"time"

	"github.com/tokenized/channels"
	"github.com/tokenized/channels/contract_operator"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type ResponseHandler interface {
	RegisterForResponse(channelID string, id uuid.UUID) <-chan peer_channels.Message
}

func NewClient(peerChannelFactory *peer_channels.Factory,
	operatorPublicKey bitcoin.PublicKey, requestPeerChannel peer_channels.Channel,
	clientKey bitcoin.Key, responsePeerChannel peer_channels.Channel,
	responseHandler ResponseHandler, responseTimeout time.Duration) (*PeerChannelsClient, error) {

	requestPeerChannelsClient, err := peerChannelFactory.NewClient(requestPeerChannel.String())
	if err != nil {
		return nil, errors.Wrap(err, "peer channels client")
	}

	return &PeerChannelsClient{
		operatorPublicKey:         operatorPublicKey,
		requestPeerChannel:        requestPeerChannel,
		requestPeerChannelsClient: requestPeerChannelsClient,
		clientKey:                 clientKey,
		responsePeerChannel:       responsePeerChannel,
		responseHandler:           responseHandler,
		responseTimeout:           responseTimeout,
	}, nil
}

type PeerChannelsClient struct {
	operatorPublicKey         bitcoin.PublicKey
	requestPeerChannel        peer_channels.Channel
	requestPeerChannelsClient peer_channels.Client
	clientKey                 bitcoin.Key
	responsePeerChannel       peer_channels.Channel
	responseHandler           ResponseHandler
	responseTimeout           time.Duration
}

// RequestNewAgent requests a new smart contract agent be created.
func (c *PeerChannelsClient) RequestNewAgent(ctx context.Context,
	adminLockingScript bitcoin.Script) (*contract_operator.Agent, error) {

	requestID := uuid.New()
	createAgent := &contract_operator.CreateAgent{
		AdminLockingScript: adminLockingScript,
	}

	requestScript, err := WrapRequest(createAgent, requestID, c.responsePeerChannel, c.clientKey)
	if err != nil {
		return nil, errors.Wrap(err, "wrap")
	}

	responseChannel := c.responseHandler.RegisterForResponse(c.responsePeerChannel.ChannelID,
		requestID)

	if err := c.requestPeerChannelsClient.WriteMessage(ctx, c.requestPeerChannel.ChannelID,
		c.requestPeerChannel.Token, peer_channels.ContentTypeBinary,
		bytes.NewReader(requestScript)); err != nil {
		return nil, errors.Wrap(err, "send")
	}

	responseMsg, err := WaitWithTimeout(responseChannel, c.responseTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "wait")
	}

	response, err := UnwrapResponse(bitcoin.Script(responseMsg.Payload))
	if err != nil {
		return nil, errors.Wrap(err, "unwrap")
	}

	if response.Signature == nil {
		return nil, errors.Wrap(ErrInvalidResponse, "missing signature")
	}

	response.Signature.SetPublicKey(&c.operatorPublicKey)
	if err := response.Signature.Verify(); err != nil {
		return nil, errors.Wrap(ErrInvalidResponse, err.Error())
	}

	if response.Response != nil && response.Response.Status != channels.StatusOK {
		return nil, errors.Wrap(ErrInvalidResponse, response.Response.Error())
	}

	createdAgent, ok := response.Msg.(*contract_operator.Agent)
	if !ok {
		return nil, errors.Wrap(ErrInvalidResponse, "not an agent")
	}

	return createdAgent, nil
}

// SignContractOffer adds a signed input and an output to a contract offer transaction.
// The input will be added as the second input so it is the contract's "operator" input.
// Then an output to retrieve the value in the input will be added.
// These have to be accounted for in the tx fee before calling this function because, since
// the input will be signed, no other changes can be made to the tx other than signing inputs
// without invalidating the operator input's signature.
func (c *PeerChannelsClient) SignContractOffer(ctx context.Context,
	etx *expanded_tx.ExpandedTx) (*expanded_tx.ExpandedTx, error) {

	requestID := uuid.New()
	signTx := &contract_operator.SignTx{
		Tx: etx,
	}

	requestScript, err := WrapRequest(signTx, requestID, c.responsePeerChannel, c.clientKey)
	if err != nil {
		return nil, errors.Wrap(err, "wrap")
	}

	responseChannel := c.responseHandler.RegisterForResponse(c.responsePeerChannel.ChannelID,
		requestID)

	if err := c.requestPeerChannelsClient.WriteMessage(ctx, c.requestPeerChannel.ChannelID,
		c.requestPeerChannel.Token, peer_channels.ContentTypeBinary,
		bytes.NewReader(requestScript)); err != nil {
		return nil, errors.Wrap(err, "send")
	}

	responseMsg, err := WaitWithTimeout(responseChannel, c.responseTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "wait")
	}

	response, err := UnwrapResponse(bitcoin.Script(responseMsg.Payload))
	if err != nil {
		return nil, errors.Wrap(err, "unwrap")
	}

	if response.Signature == nil {
		return nil, errors.Wrap(ErrInvalidResponse, "missing signature")
	}

	response.Signature.SetPublicKey(&c.operatorPublicKey)
	if err := response.Signature.Verify(); err != nil {
		return nil, errors.Wrap(ErrInvalidResponse, err.Error())
	}

	if response.Response != nil && response.Response.Status != channels.StatusOK {
		return nil, errors.Wrap(ErrInvalidResponse, response.Response.Error())
	}

	signedTx, ok := response.Msg.(*contract_operator.SignedTx)
	if !ok {
		return nil, errors.Wrap(ErrInvalidResponse, "not a signed tx")
	}

	return signedTx.Tx, nil
}

func WaitWithTimeout(responseChannel <-chan peer_channels.Message,
	timeout time.Duration) (*peer_channels.Message, error) {

	select {
	case response := <-responseChannel:
		return &response, nil
	case <-time.After(timeout):
		return nil, errors.Wrap(ErrTimeout, timeout.String())
	}
}
