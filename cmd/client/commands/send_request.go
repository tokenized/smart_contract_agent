package commands

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/tokenized/channels"
	channelsExpandedTx "github.com/tokenized/channels/expanded_tx"
	envelopeV1 "github.com/tokenized/envelope/pkg/golang/envelope/v1"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/smart_contract_agent/pkg/client"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var SendRequest = &cobra.Command{
	Use:   "send_request agent_peer_channel response_write_peer_channel response_read_token expanded_tx",
	Short: "Sends a request to the smart contract agent",
	Args:  cobra.ExactArgs(4),
	RunE: func(c *cobra.Command, args []string) error {
		ctx := logger.ContextWithLogger(context.Background(), true, true, "")

		agentPeerChannel, err := peer_channels.ParseChannel(args[0])
		if err != nil {
			return errors.Wrap(err, "agent peer channel")
		}

		responsePeerChannel, err := peer_channels.ParseChannel(args[1])
		if err != nil {
			return errors.Wrap(err, "response peer channel")
		}

		responseReadToken := args[2]

		etx, err := decodeExpandedTx(args[3])
		if err != nil {
			return errors.Wrap(err, "tx")
		}

		peerChannelsFactory := peer_channels.NewFactory()

		if err = sendRequest(ctx, peerChannelsFactory, agentPeerChannel, responsePeerChannel,
			etx); err != nil {
			return errors.Wrap(err, "request")
		}

		if err = waitForResponse(ctx, peerChannelsFactory, responsePeerChannel, responseReadToken,
			etx.TxID()); err != nil {
			return errors.Wrap(err, "response")
		}

		return nil
	},
}

func decodeExpandedTx(arg string) (*expanded_tx.ExpandedTx, error) {
	b, err := hex.DecodeString(arg)
	if err != nil {
		return nil, errors.Wrap(err, "hex")
	}

	payload, err := envelopeV1.Parse(bytes.NewReader(b))
	if err != nil {
		return nil, errors.Wrap(err, "envelope")
	}

	if len(payload.ProtocolIDs) == 0 {
		return nil, errors.Wrap(channels.ErrUnsupportedProtocol, "no data protocol")
	}

	if len(payload.ProtocolIDs) > 1 {
		return nil, errors.Wrap(channels.ErrUnsupportedProtocol, "more than one data protocol")
	}

	msg, _, err := channelsExpandedTx.Parse(payload)
	if err != nil {
		return nil, errors.Wrap(err, "etx")
	}

	if msg == nil {
		return nil, errors.New("Expanded tx not found")
	}

	if cetx, ok := msg.(*channelsExpandedTx.ExpandedTxMessage); ok {
		etx := expanded_tx.ExpandedTx(*cetx)
		return &etx, nil
	}

	return nil, channels.ErrUnsupportedProtocol
}

func sendRequest(ctx context.Context, peerChannelsFactory *peer_channels.Factory,
	requestPeerChannel, responsePeerChannel *peer_channels.Channel,
	etx *expanded_tx.ExpandedTx) error {

	peerChannelsClient, err := peerChannelsFactory.NewClient(requestPeerChannel.BaseURL)
	if err != nil {
		return errors.Wrapf(err, "peer channel client: %s", requestPeerChannel.BaseURL)
	}

	script, err := client.WrapRequest(etx, responsePeerChannel)
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

func waitForResponse(ctx context.Context, peerChannelsFactory *peer_channels.Factory,
	peerChannel *peer_channels.Channel, readToken string, txid bitcoin.Hash32) error {

	peerChannelsClient, err := peerChannelsFactory.NewClient(peerChannel.BaseURL)
	if err != nil {
		return errors.Wrapf(err, "peer channel client: %s", peerChannel.BaseURL)
	}

	incoming := make(chan peer_channels.Message, 10)
	var wait sync.WaitGroup

	peerChannelThread, peerChannelComplete := threads.NewInterruptableThreadComplete("Peer Channel Listen",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return peerChannelsClient.Listen(ctx, readToken, true, incoming, interrupt)
		}, &wait)

	handleMessagesThread, handleMessagesComplete := threads.NewUninterruptableThreadComplete("Handle Messages",
		func(ctx context.Context) error {
			return handleResponseMessages(ctx, peerChannelsClient, readToken, txid, incoming)
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
	return nil
}

func handleResponseMessages(ctx context.Context, peerChannelsClient peer_channels.Client,
	token string, txid bitcoin.Hash32, incoming <-chan peer_channels.Message) error {

	for msg := range incoming {
		peerChannelsClient.MarkMessages(ctx, msg.ChannelID, token, msg.Sequence, true, true)

		if msg.ContentType != peer_channels.ContentTypeBinary {
			return errors.Wrap(peer_channels.ErrWrongContentType, msg.ContentType)
		}

		response, err := client.UnwrapResponse(bitcoin.Script(msg.Payload))
		if err != nil {
			return errors.Wrap(err, "unwrap")
		}

		if response.TxID != nil {
			if response.TxID.Equal(&txid) {
				if response.Response != nil {
					println("Received response :", response.Response.Error())
				} else {
					println("Received empty response")
				}

				return nil
			}
		} else if response.Tx != nil {
			println("Received tx :", response.Tx.String())

			for _, txin := range response.Tx.Tx.TxIn {
				if txin.PreviousOutPoint.Hash.Equal(&txid) {
					return nil
				}
			}
		}
	}

	return nil
}
