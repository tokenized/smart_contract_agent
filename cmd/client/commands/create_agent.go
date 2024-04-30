package commands

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/tokenized/channels"
	"github.com/tokenized/channels/contract_operator"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/smart_contract_agent/pkg/operator_client"
	"github.com/tokenized/threads"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var CreateAgent = &cobra.Command{
	Use:   "create_agent operator_peer_channel response_write_peer_channel response_read_token client_key admin_script_hex",
	Short: "Sends a request to the smart contract agent operator to create a new smart contract agent",
	Args:  cobra.ExactArgs(5),
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

		b, err := hex.DecodeString(args[4])
		if err != nil {
			return errors.Wrap(err, "admin_script_hex")
		}
		adminLockingScript := bitcoin.Script(b)

		if !adminLockingScript.IsP2PKH() {
			// Other script types are supported, but this is a safety error in case the hex provided
			// was malformed.
			return errors.Wrap(errors.New("Script is not P2PKH"), "admin_script_hex")
		}

		id := uuid.New()

		msg := &contract_operator.CreateAgent{
			AdminLockingScript: adminLockingScript,
		}

		if err = sendOperatorRequest(ctx, peerChannelsFactory, *operatorPeerChannel,
			*responsePeerChannel, clientKey, id, msg); err != nil {
			return errors.Wrap(err, "request")
		}

		if _, err = waitForOperatorResponse(ctx, peerChannelsFactory, *responsePeerChannel,
			responseReadToken, id); err != nil {
			return errors.Wrap(err, "response")
		}

		return nil
	},
}

func sendOperatorRequest(ctx context.Context, peerChannelsFactory *peer_channels.Factory,
	requestPeerChannel, responsePeerChannel peer_channels.Channel, clientKey bitcoin.Key,
	id uuid.UUID, msg channels.Writer) error {

	peerChannelsClient, err := peerChannelsFactory.NewClient(requestPeerChannel.BaseURL)
	if err != nil {
		return errors.Wrapf(err, "peer channel client: %s", requestPeerChannel.BaseURL)
	}

	script, err := operator_client.WrapRequest(msg, id, responsePeerChannel, clientKey)
	if err != nil {
		return errors.Wrap(err, "wrap")
	}

	if err := peerChannelsClient.WriteMessage(ctx, requestPeerChannel.ChannelID,
		requestPeerChannel.Token, peer_channels.ContentTypeBinary,
		bytes.NewReader(script)); err != nil {
		return errors.Wrap(err, "write message")
	}

	return nil
}

func waitForOperatorResponse(ctx context.Context, peerChannelsFactory *peer_channels.Factory,
	peerChannel peer_channels.Channel, readToken string, id uuid.UUID) (*operator_client.Response, error) {

	peerChannelsClient, err := peerChannelsFactory.NewClient(peerChannel.BaseURL)
	if err != nil {
		return nil, errors.Wrapf(err, "peer channel client: %s", peerChannel.BaseURL)
	}

	incoming := make(chan peer_channels.Message, 10)
	var wait sync.WaitGroup

	peerChannelThread, peerChannelComplete := threads.NewInterruptableThreadComplete("Peer Channel Listen",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return peerChannelsClient.Listen(ctx, readToken, true, time.Second, incoming, interrupt)
		}, &wait)

	var response *operator_client.Response
	handleMessagesThread, handleMessagesComplete := threads.NewUninterruptableThreadComplete("Handle Messages",
		func(ctx context.Context) error {
			var err error
			response, err = handleOperatorResponseMessages(ctx, peerChannelsClient, readToken, id, incoming)
			return err
		}, &wait)

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM)

	peerChannelThread.Start(ctx)
	handleMessagesThread.Start(ctx)

	select {
	case err := <-handleMessagesComplete:
		if err != nil {
			logger.Error(ctx, "Peer Channel Listen completed : %s", err)
		}

	case err := <-peerChannelComplete:
		logger.Error(ctx, "Peer Channel Listen completed : %s", err)

	case <-osSignals:
		logger.Info(ctx, "Start shutdown")
	}

	peerChannelThread.Stop(ctx)
	close(incoming)

	wait.Wait()
	return response, nil
}

func handleOperatorResponseMessages(ctx context.Context, peerChannelsClient peer_channels.Client,
	token string, id uuid.UUID, incoming <-chan peer_channels.Message) (*operator_client.Response, error) {

	for msg := range incoming {
		peerChannelsClient.MarkMessages(ctx, msg.ChannelID, token, msg.Sequence, true, true)

		if msg.ContentType != peer_channels.ContentTypeBinary {
			return nil, errors.Wrap(peer_channels.ErrWrongContentType, msg.ContentType)
		}

		response, err := operator_client.UnwrapResponse(bitcoin.Script(msg.Payload))
		if err != nil {
			return nil, errors.Wrap(err, "unwrap")
		}

		if response.ID == nil {
			continue
		}

		if !bytes.Equal(response.ID[:], id[:]) {
			continue
		}

		if signedTx, ok := response.Msg.(*contract_operator.SignedTx); ok {
			fmt.Printf("Expanded Tx: %s\n", signedTx.Tx)
		}

		return response, nil
	}

	return nil, nil // incoming closed
}
