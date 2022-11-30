package client

import (
	"github.com/tokenized/channels"
	envelopeV1 "github.com/tokenized/envelope/pkg/golang/envelope/v1"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/peer_channels"

	"github.com/pkg/errors"
)

func Wrap(msg channels.Writer,
	replyPeerChannel *peer_channels.PeerChannel) (bitcoin.Script, error) {

	payload, err := msg.Write()
	if err != nil {
		return nil, errors.Wrap(err, "write")
	}

	if replyPeerChannel != nil {
		replyTo := &channels.ReplyTo{
			PeerChannel: replyPeerChannel,
		}

		payload, err = replyTo.Wrap(payload)
		if err != nil {
			return nil, errors.Wrap(err, "reply to")
		}
	}

	return envelopeV1.Wrap(payload).Script()
}

func Unwrap(msg channels.Writer) {

}
