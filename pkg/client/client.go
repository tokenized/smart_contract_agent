package client

import (
	"bytes"

	"github.com/tokenized/channels"
	channelsExpandedTx "github.com/tokenized/channels/expanded_tx"
	channelsWallet "github.com/tokenized/channels/wallet"
	envelopeV1 "github.com/tokenized/envelope/pkg/golang/envelope/v1"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"

	"github.com/pkg/errors"
)

type Request struct {
	ReplyTo *channels.ReplyTo
	Tx      *expanded_tx.ExpandedTx
}

type Response struct {
	Signature *channels.Signature
	Response  *channels.Response
	TxID      *bitcoin.Hash32
	Tx        *expanded_tx.ExpandedTx
}

func WrapRequest(etx *expanded_tx.ExpandedTx,
	replyPeerChannel *peer_channels.PeerChannel) (bitcoin.Script, error) {

	cetx := channelsExpandedTx.ExpandedTxMessage(*etx)

	payload, err := cetx.Write()
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

func UnwrapRequest(script bitcoin.Script) (*Request, error) {
	result := &Request{}

	payload, err := envelopeV1.Parse(bytes.NewReader(script))
	if err != nil {
		return nil, errors.Wrap(err, "envelope")
	}

	replyTo, payload, err := channels.ParseReplyTo(payload)
	if err != nil {
		return nil, errors.Wrap(err, "reply to")
	}
	result.ReplyTo = replyTo

	if len(payload.ProtocolIDs) == 0 {
		return nil, errors.Wrap(channels.ErrUnsupportedProtocol, "no data protocol")
	}

	if len(payload.ProtocolIDs) > 1 {
		return nil, errors.Wrap(channels.ErrUnsupportedProtocol, "more than one data protocol")
	}

	msg, err := channelsExpandedTx.Parse(payload)
	if err != nil {
		return nil, errors.Wrap(err, "etx")
	}

	if msg != nil {
		if cetx, ok := msg.(*channelsExpandedTx.ExpandedTxMessage); ok {
			etx := expanded_tx.ExpandedTx(*cetx)
			result.Tx = &etx
		} else {
			return nil, channels.ErrUnsupportedProtocol
		}
	}

	return result, nil
}

func WrapTxIDResponse(txid bitcoin.Hash32, response *channels.Response,
	key *bitcoin.Key) (bitcoin.Script, error) {

	ctxid := channels.TxID(txid)

	payload, err := ctxid.Write()
	if err != nil {
		return nil, errors.Wrap(err, "write")
	}

	if response != nil {
		payload, err = response.Wrap(payload)
		if err != nil {
			return nil, errors.Wrap(err, "response")
		}
	}

	if key != nil {
		hash := channelsWallet.RandomHash()
		payload, err = channels.WrapSignature(payload, *key, &hash, false)
		if err != nil {
			return nil, errors.Wrap(err, "signature")
		}
	}

	return envelopeV1.Wrap(payload).Script()
}

func WrapExpandedTxResponse(etx *expanded_tx.ExpandedTx) (bitcoin.Script, error) {
	cetx := channelsExpandedTx.ExpandedTxMessage(*etx)

	payload, err := cetx.Write()
	if err != nil {
		return nil, errors.Wrap(err, "write")
	}

	return envelopeV1.Wrap(payload).Script()
}

func UnwrapResponse(script bitcoin.Script) (*Response, error) {
	result := &Response{}

	payload, err := envelopeV1.Parse(bytes.NewReader(script))
	if err != nil {
		return nil, errors.Wrap(err, "envelope")
	}

	signature, payload, err := channels.ParseSigned(payload)
	if err != nil {
		return nil, errors.Wrap(err, "sign")
	}
	result.Signature = signature

	response, payload, err := channels.ParseResponse(payload)
	if err != nil {
		return nil, errors.Wrap(err, "response")
	}
	result.Response = response

	if len(payload.ProtocolIDs) == 0 {
		return nil, errors.Wrap(channels.ErrUnsupportedProtocol, "no data protocol")
	}

	if len(payload.ProtocolIDs) > 1 {
		return nil, errors.Wrap(channels.ErrUnsupportedProtocol, "more than one data protocol")
	}

	txid, payload, err := channels.ParseTxID(payload)
	if err != nil {
		return nil, errors.Wrap(err, "etx")
	}
	result.TxID = txid

	expandedTx, err := channelsExpandedTx.Parse(payload)
	if err != nil {
		return nil, errors.Wrap(err, "etx")
	}

	if expandedTx != nil {
		if cetx, ok := expandedTx.(*channelsExpandedTx.ExpandedTxMessage); ok {
			etx := expanded_tx.ExpandedTx(*cetx)
			result.Tx = &etx
			return result, nil
		} else {
			return nil, channels.ErrUnsupportedProtocol
		}
	}

	return result, nil
}
