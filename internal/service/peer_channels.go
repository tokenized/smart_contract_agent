package service

import (
	"context"
	"sync"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

func (s *Service) PeerChannelListen(ctx context.Context, interrupt <-chan interface{},
	peerChannel *peer_channels.Channel, readToken *string) error {
	if peerChannel == nil || len(peerChannel.BaseURL) == 0 || readToken == nil {
		logger.Info(ctx, "No incoming peer channel specified")

		// Just wait for interrupt
		select {
		case <-interrupt:
			return nil
		}
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.String("peer_channel_url", peerChannel.MaskedString()),
	}, "Listening for incoming peer channel messages")

	peerChannelClient, err := s.peerChannelsFactory.NewClient(peerChannel.BaseURL)
	if err != nil {
		return errors.Wrap(err, "peer channel client")
	}

	var wait sync.WaitGroup
	incoming := make(chan peer_channels.Message, 1000)

	listenThread, listenComplete := threads.NewInterruptableThreadComplete("Listen",
		func(ctx context.Context, interrupt <-chan interface{}) error {
			return listenPeerChannel(ctx, peerChannelClient, *peerChannel, *readToken, incoming,
				interrupt)
		}, &wait)

	handleThread, handleComplete := threads.NewUninterruptableThreadComplete("Handle",
		func(ctx context.Context) error {
			return s.handlePeerChannelMessages(ctx, peerChannelClient, *readToken, incoming)
		}, &wait)

	listenThread.Start(ctx)
	handleThread.Start(ctx)

	select {
	case err := <-listenComplete:
		logger.Error(ctx, "Listen to peer channels completed : %s", err)

	case err := <-handleComplete:
		logger.Error(ctx, "Handle peer channel messages completed : %s", err)

	case <-interrupt:
	}

	listenThread.Stop(ctx)
	close(incoming)

	wait.Wait()
	return threads.CombineErrors(listenThread.Error(), handleThread.Error())
}

func (s *Service) handlePeerChannelMessages(ctx context.Context, client peer_channels.Client,
	token string, incoming <-chan peer_channels.Message) error {

	for msg := range incoming {
		if err := s.agent.ProcessPeerChannelMessage(ctx, msg); err != nil {
			return errors.Wrap(err, "process message")
		}

		if err := client.MarkMessages(ctx, msg.ChannelID, token, msg.Sequence, true,
			true); err != nil {
			return errors.Wrap(err, "mark message as read")
		}
	}

	return nil
}

func listenPeerChannel(ctx context.Context, client peer_channels.Client,
	peerChannel peer_channels.Channel, token string, incoming chan<- peer_channels.Message,
	interrupt <-chan interface{}) error {

	for {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.String("peer_channel", peerChannel.MaskedString()),
		}, "Connecting to peer channel service to listen for messages")

		if err := client.Listen(ctx, token, true, incoming, interrupt); err != nil {
			if errors.Cause(err) == threads.Interrupted {
				return nil
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.String("peer_channel", peerChannel.MaskedString()),
			}, "Peer channel listening returned with error : %s", err)
		} else {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.String("peer_channel", peerChannel.MaskedString()),
			}, "Peer channel listening returned")
		}

		logger.WarnWithFields(ctx, []logger.Field{
			logger.String("peer_channel", peerChannel.MaskedString()),
		}, "Waiting to reconnect to Peer channel")
		select {
		case <-time.After(time.Second * 5):
		case <-interrupt:
			return nil
		}
	}
}
