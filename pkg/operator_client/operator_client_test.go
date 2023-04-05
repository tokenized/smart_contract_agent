package operator_client

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/tokenized/channels/peer_channels_listener"
	"github.com/tokenized/channels/uuid_response_handler"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/wire"
)

// Test_Interface ensures that the PeerChannelsClient fulfills the Client interface.
func Test_Interface(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")

	peerChannelFactory := peer_channels.NewFactory()
	peerChannelsClient := peerChannelFactory.MockClient()

	operatorKey, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	requestAccount, _ := peerChannelsClient.CreateAccount(ctx)
	requestAccountClient := peer_channels.NewMockAccountClient(peerChannelsClient,
		requestAccount.AccountID, requestAccount.Token)
	requestPeerChannelFull, _ := requestAccountClient.CreatePublicChannel(ctx)
	requestPeerChannel, _ := peer_channels.NewChannel(peer_channels.MockClientURL,
		requestPeerChannelFull.ID, requestPeerChannelFull.WriteToken)

	clientKey, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	responseAccount, _ := peerChannelsClient.CreateAccount(ctx)
	responseAccountClient := peer_channels.NewMockAccountClient(peerChannelsClient,
		responseAccount.AccountID, responseAccount.Token)
	responsePeerChannelFull, _ := responseAccountClient.CreatePublicChannel(ctx)
	responsePeerChannel, _ := peer_channels.NewChannel(peer_channels.MockClientURL,
		responsePeerChannelFull.ID, responsePeerChannelFull.WriteToken)

	client, err := NewClient(peerChannelFactory, operatorKey.PublicKey(), *requestPeerChannel,
		clientKey, *responsePeerChannel, nil, time.Second)
	if err != nil {
		t.Fatalf("Failed to create new client : %s", err)
	}

	var clientInterface Client
	clientInterface = client

	if clientInterface == nil {
		t.Fatalf("Client interface is nil")
	}
}

func Test_RequestNewAgent(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")

	peerChannelFactory := peer_channels.NewFactory()
	peerChannelsClient := peerChannelFactory.MockClient()

	operatorKey, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	requestAccount, _ := peerChannelsClient.CreateAccount(ctx)
	requestAccountClient := peer_channels.NewMockAccountClient(peerChannelsClient,
		requestAccount.AccountID, requestAccount.Token)
	requestPeerChannelFull, _ := requestAccountClient.CreatePublicChannel(ctx)
	requestPeerChannel, _ := peer_channels.NewChannel(peer_channels.MockClientURL,
		requestPeerChannelFull.ID, requestPeerChannelFull.WriteToken)

	clientKey, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	responseAccount, _ := peerChannelsClient.CreateAccount(ctx)
	responseAccountClient := peer_channels.NewMockAccountClient(peerChannelsClient,
		responseAccount.AccountID, responseAccount.Token)
	responsePeerChannelFull, _ := responseAccountClient.CreatePublicChannel(ctx)
	responsePeerChannel, _ := peer_channels.NewChannel(peer_channels.MockClientURL,
		responsePeerChannelFull.ID, responsePeerChannelFull.WriteToken)

	responseHandler := uuid_response_handler.NewHandler()
	responseListener := peer_channels_listener.NewPeerChannelsListener(peerChannelsClient,
		responseAccount.Token, 100, responseHandler.HandleMessage, responseHandler.HandleUpdate)
	responseHandler.SetAddUpdate(responseListener.AddUpdate)

	client, err := NewClient(peerChannelFactory, operatorKey.PublicKey(), *requestPeerChannel,
		clientKey, *responsePeerChannel, responseHandler, time.Second)
	if err != nil {
		t.Fatalf("Failed to create new client : %s", err)
	}

	mockOperator := NewMockOperator(peerChannelFactory, operatorKey, clientKey.PublicKey(),
		requestPeerChannelFull.ID, 1000)

	operatorListener := peer_channels_listener.NewPeerChannelsListener(peerChannelsClient,
		requestAccount.Token, 100, mockOperator.HandleMessage, nil)

	operatorListenerInterrupt := make(chan interface{})
	operatorListenerComplete := make(chan interface{})
	var operatorListenerError error
	go func() {
		operatorListenerError = operatorListener.Run(ctx, operatorListenerInterrupt)
		close(operatorListenerComplete)
	}()

	responseListenerInterrupt := make(chan interface{})
	responseListenerComplete := make(chan interface{})
	var responseListenerError error
	go func() {
		responseListenerError = responseListener.Run(ctx, responseListenerInterrupt)
		close(responseListenerComplete)
	}()

	adminKey, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	adminLockingScript, _ := adminKey.LockingScript()

	responseAgent, err := client.RequestNewAgent(ctx, adminLockingScript)
	if err != nil {
		t.Fatalf("Failed to request new agent : %s", err)
	}

	js, _ := json.MarshalIndent(responseAgent, "", "  ")
	t.Logf("Agent : %s", js)

	close(operatorListenerInterrupt)
	select {
	case <-operatorListenerComplete:
	case <-time.After(time.Second):
		t.Fatalf("Operator listener shutdown timed out")
	}

	close(responseListenerInterrupt)
	select {
	case <-responseListenerComplete:
	case <-time.After(time.Second):
		t.Fatalf("Response listener shutdown timed out")
	}

	if operatorListenerError != nil {
		t.Fatalf("Operator listener completed with error : %s", operatorListenerError)
	}

	if responseListenerError != nil {
		t.Fatalf("Response listener completed with error : %s", responseListenerError)
	}
}

func Test_SignContractOffer(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")

	peerChannelFactory := peer_channels.NewFactory()
	peerChannelsClient := peerChannelFactory.MockClient()

	operatorKey, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	requestAccount, _ := peerChannelsClient.CreateAccount(ctx)
	requestAccountClient := peer_channels.NewMockAccountClient(peerChannelsClient,
		requestAccount.AccountID, requestAccount.Token)
	requestPeerChannelFull, _ := requestAccountClient.CreatePublicChannel(ctx)
	requestPeerChannel, _ := peer_channels.NewChannel(peer_channels.MockClientURL,
		requestPeerChannelFull.ID, requestPeerChannelFull.WriteToken)

	clientKey, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	responseAccount, _ := peerChannelsClient.CreateAccount(ctx)
	responseAccountClient := peer_channels.NewMockAccountClient(peerChannelsClient,
		responseAccount.AccountID, responseAccount.Token)
	responsePeerChannelFull, _ := responseAccountClient.CreatePublicChannel(ctx)
	responsePeerChannel, _ := peer_channels.NewChannel(peer_channels.MockClientURL,
		responsePeerChannelFull.ID, responsePeerChannelFull.WriteToken)

	responseHandler := uuid_response_handler.NewHandler()
	responseListener := peer_channels_listener.NewPeerChannelsListener(peerChannelsClient,
		responseAccount.Token, 100, responseHandler.HandleMessage, responseHandler.HandleUpdate)
	responseHandler.SetAddUpdate(responseListener.AddUpdate)

	client, err := NewClient(peerChannelFactory, operatorKey.PublicKey(), *requestPeerChannel,
		clientKey, *responsePeerChannel, responseHandler, time.Second)
	if err != nil {
		t.Fatalf("Failed to create new client : %s", err)
	}

	mockOperator := NewMockOperator(peerChannelFactory, operatorKey, clientKey.PublicKey(),
		requestPeerChannelFull.ID, 1000)

	operatorListener := peer_channels_listener.NewPeerChannelsListener(peerChannelsClient,
		requestAccount.Token, 100, mockOperator.HandleMessage, nil)

	operatorListenerInterrupt := make(chan interface{})
	operatorListenerComplete := make(chan interface{})
	var operatorListenerError error
	go func() {
		operatorListenerError = operatorListener.Run(ctx, operatorListenerInterrupt)
		close(operatorListenerComplete)
	}()

	responseListenerInterrupt := make(chan interface{})
	responseListenerComplete := make(chan interface{})
	var responseListenerError error
	go func() {
		responseListenerError = responseListener.Run(ctx, responseListenerInterrupt)
		close(responseListenerComplete)
	}()

	authTx := wire.NewMsgTx(1)

	authInput := &wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Index: uint32(rand.Intn(10)),
		},
		Sequence: wire.MaxTxInSequenceNum,
	}
	rand.Read(authInput.PreviousOutPoint.Hash[:])

	adminKey, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	adminLockingScript, _ := adminKey.LockingScript()

	authTx.AddTxIn(authInput)
	authTx.AddTxOut(wire.NewTxOut(2000, adminLockingScript))

	tx := wire.NewMsgTx(1)

	contractKey, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	contractLockingScript, _ := contractKey.LockingScript()

	tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(authTx.TxHash(), 0), nil))
	tx.AddTxOut(wire.NewTxOut(1025, contractLockingScript))

	etx := &expanded_tx.ExpandedTx{
		Tx: tx,
		Ancestors: expanded_tx.AncestorTxs{
			{
				Tx: authTx,
			},
		},
	}

	signedTx, err := client.SignContractOffer(ctx, etx)
	if err != nil {
		t.Fatalf("Failed to get signed tx : %s", err)
	}

	t.Logf("Signed Tx : %s", signedTx)

	close(operatorListenerInterrupt)
	select {
	case <-operatorListenerComplete:
	case <-time.After(time.Second):
		t.Fatalf("Operator listener shutdown timed out")
	}

	close(responseListenerInterrupt)
	select {
	case <-responseListenerComplete:
	case <-time.After(time.Second):
		t.Fatalf("Response listener shutdown timed out")
	}

	if operatorListenerError != nil {
		t.Fatalf("Operator listener completed with error : %s", operatorListenerError)
	}

	if responseListenerError != nil {
		t.Fatalf("Response listener completed with error : %s", responseListenerError)
	}
}
