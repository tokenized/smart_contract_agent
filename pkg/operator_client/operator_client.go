package operator_client

import (
	"github.com/tokenized/channels"
	"github.com/tokenized/channels/contract_operator"
	channelsWallet "github.com/tokenized/channels/wallet"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/peer_channels"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type Request struct {
	Signature *channels.Signature
	ReplyTo   *channels.ReplyTo
	ID        *uuid.UUID
	Msg       channels.Message
}

type Response struct {
	Signature *channels.Signature
	Response  *channels.Response
	ID        *uuid.UUID
	Msg       channels.Message
}

func WrapRequest(msg channels.Writer, id uuid.UUID, replyPeerChannel peer_channels.Channel,
	key bitcoin.Key) (bitcoin.Script, error) {

	var replyTo *channels.ReplyTo
	replyTo = &channels.ReplyTo{
		PeerChannel: &replyPeerChannel,
	}

	signature := channels.NewSignature(key, channelsWallet.RandomHashPtr(), false)

	return channels.Wrap(msg, replyTo, signature)
}

func UnwrapRequest(script bitcoin.Script) (*Request, error) {
	protocols := channels.NewProtocols(channels.NewSignedProtocol(),
		channels.NewReplyToProtocol(), contract_operator.NewProtocol())

	msg, wrappers, err := protocols.Parse(script)
	if err != nil {
		return nil, errors.Wrap(err, "parse")
	}

	result := &Request{}

	if len(wrappers) > 0 {
		if sig, ok := wrappers[0].(*channels.Signature); ok {
			result.Signature = sig
			wrappers = wrappers[1:]
		}
	}

	if len(wrappers) > 0 {
		if reply, ok := wrappers[0].(*channels.ReplyTo); ok {
			result.ReplyTo = reply
			wrappers = wrappers[1:]
		}
	}

	if len(wrappers) > 0 {
		if id, ok := wrappers[0].(*channels.UUID); ok {
			uuid := uuid.UUID(*id)
			result.ID = &uuid
			wrappers = wrappers[1:]
		}
	}

	if len(wrappers) > 0 {
		return nil, errors.Wrapf(channels.ErrUnsupportedProtocol, "%s",
			wrappers[0].ProtocolID())
	}

	result.Msg = msg
	return result, nil
}

func WrapResponse(msg channels.Writer, id uuid.UUID, response *channels.Response,
	key bitcoin.Key) (bitcoin.Script, error) {

	signature := channels.NewSignature(key, channelsWallet.RandomHashPtr(), false)
	uuid := channels.UUID(id)
	return channels.Wrap(msg, &uuid, response, signature)
}

func UnwrapResponse(script bitcoin.Script) (*Response, error) {
	protocols := channels.NewProtocols(channels.NewSignedProtocol(),
		channels.NewResponseProtocol(), contract_operator.NewProtocol())

	msg, wrappers, err := protocols.Parse(script)
	if err != nil {
		return nil, errors.Wrap(err, "parse")
	}

	result := &Response{}

	if len(wrappers) > 0 {
		if sig, ok := wrappers[0].(*channels.Signature); ok {
			result.Signature = sig
			wrappers = wrappers[1:]
		}
	}

	if len(wrappers) > 0 {
		if res, ok := wrappers[0].(*channels.Response); ok {
			result.Response = res
			wrappers = wrappers[1:]
		}
	}

	if len(wrappers) > 0 {
		if id, ok := wrappers[0].(*channels.UUID); ok {
			uuid := uuid.UUID(*id)
			result.ID = &uuid
			wrappers = wrappers[1:]
		}
	}

	if len(wrappers) > 0 {
		return nil, errors.Wrapf(channels.ErrUnsupportedProtocol, "%s",
			wrappers[0].ProtocolID())
	}

	result.Msg = msg
	return result, nil
}
