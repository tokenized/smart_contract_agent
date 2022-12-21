package client

import (
	"github.com/tokenized/channels"
	channelsExpandedTx "github.com/tokenized/channels/expanded_tx"
	channelsWallet "github.com/tokenized/channels/wallet"
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
	replyPeerChannel *peer_channels.Channel) (bitcoin.Script, error) {

	cetx := channelsExpandedTx.ExpandedTxMessage(*etx)

	var replyTo *channels.ReplyTo
	if replyPeerChannel != nil {
		replyTo = &channels.ReplyTo{
			PeerChannel: replyPeerChannel,
		}
	}

	return channels.Wrap(&cetx, replyTo)
}

func UnwrapRequest(script bitcoin.Script) (*Request, error) {
	protocols := channels.NewProtocols(channelsExpandedTx.NewProtocol(),
		channels.NewReplyToProtocol())

	msg, wrappers, err := protocols.Parse(script)
	if err != nil {
		return nil, errors.Wrap(err, "parse")
	}

	if len(wrappers) > 1 {
		return nil, errors.Wrap(channels.ErrUnsupportedProtocol, "more than one wrapper")
	}

	result := &Request{}
	if len(wrappers) == 1 {
		if rt, ok := wrappers[0].(*channels.ReplyTo); ok {
			result.ReplyTo = rt
		} else {
			return nil, errors.Wrapf(channels.ErrUnsupportedProtocol, "%s",
				wrappers[0].ProtocolID())
		}
	}

	if cetx, ok := msg.(*channelsExpandedTx.ExpandedTxMessage); ok {
		etx := expanded_tx.ExpandedTx(*cetx)
		result.Tx = &etx
	} else {
		return nil, errors.Wrapf(channels.ErrUnsupportedProtocol, "%s", msg.ProtocolID())
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

	var signature *channels.Signature
	if key != nil {
		hash := channelsWallet.RandomHash()
		signature = channels.NewSignature(*key, &hash, false)
	}

	return channels.Wrap(&ctxid, response, signature)
}

func WrapExpandedTxResponse(etx *expanded_tx.ExpandedTx) (bitcoin.Script, error) {
	cetx := channelsExpandedTx.ExpandedTxMessage(*etx)
	return channels.Wrap(&cetx)
}

func UnwrapResponse(script bitcoin.Script) (*Response, error) {
	protocols := channels.NewProtocols(channelsExpandedTx.NewProtocol(), channels.NewTxIDProtocol(),
		channels.NewResponseProtocol(), channels.NewSignedProtocol())

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
		return nil, errors.Wrapf(channels.ErrUnsupportedProtocol, "%s",
			wrappers[0].ProtocolID())
	}

	if ctxid, ok := msg.(*channels.TxID); ok {
		txid := bitcoin.Hash32(*ctxid)
		result.TxID = &txid
	} else if cetx, ok := msg.(*channelsExpandedTx.ExpandedTxMessage); ok {
		etx := expanded_tx.ExpandedTx(*cetx)
		result.Tx = &etx
	} else {
		return nil, errors.Wrapf(channels.ErrUnsupportedProtocol, "%s", msg.ProtocolID())
	}

	return result, nil
}
