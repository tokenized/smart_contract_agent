package agents

import (
	"bytes"
	"context"
	"time"

	"github.com/tokenized/channels"
	channelsExpandedTx "github.com/tokenized/channels/expanded_tx"
	envelope "github.com/tokenized/envelope/pkg/golang/envelope/base"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/client"
	spyNodeClient "github.com/tokenized/spynode/pkg/client"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

func (a *Agent) ProcessPeerChannelMessage(ctx context.Context, msg peer_channels.Message) error {
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("trace", uuid.New()))

	logger.InfoWithFields(ctx, []logger.Field{
		logger.String("peer_channel", msg.ChannelID),
		logger.Uint64("sequence", msg.Sequence),
	}, "Processing peer channel message")

	if msg.ContentType != peer_channels.ContentTypeBinary {
		return errors.Wrap(peer_channels.ErrWrongContentType, msg.ContentType)
	}

	request, err := client.UnwrapRequest(bitcoin.Script(msg.Payload))
	if err != nil {
		return errors.Wrap(err, "unwrap request")
	}

	txid := request.Tx.TxID()

	// Verify that expanded tx inputs were provided.
	if err := request.Tx.VerifyAncestors(); err != nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("request_txid", txid),
		}, "Received peer channel transaction is missing ancestors : %s", err)

		if err := a.sendPeerChannelReject(ctx, request.ReplyTo, request.Tx, channels.StatusReject,
			channelsExpandedTx.ProtocolID, channelsExpandedTx.ResponseCodeMissingAncestors,
			err.Error()); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("request_txid", txid),
				logger.String("peer_channel", request.ReplyTo.PeerChannel.MaskedString()),
			}, "Failed to send peer channel reject response : %s", err)
		}

		return nil
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("request_txid", txid),
	}, "Received peer channel message with transaction")

	agentLockingScript := a.LockingScript()

	config := a.Config()
	requestOutputs, err := relevantRequestOutputs(ctx, request.Tx, agentLockingScript, config.IsTest)
	if err != nil {
		return errors.Wrap(err, "tx is relevant")
	}

	if len(requestOutputs) == 0 {
		logger.Warn(ctx, "Transaction is not relevant")

		if err := a.sendPeerChannelReject(ctx, request.ReplyTo, request.Tx,
			channels.StatusReject, client.ProtocolID, client.ResponseCodeNotRelevant,
			""); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("request_txid", txid),
				logger.String("peer_channel", request.ReplyTo.PeerChannel.MaskedString()),
			}, "Failed to send peer channel reject response : %s", err)
		}

		return nil
	}

	if request.ReplyTo != nil && request.ReplyTo.PeerChannel != nil {
		if err := a.AddResponder(ctx, txid, request.ReplyTo.PeerChannel); err != nil {
			return errors.Wrap(err, "add responder")
		}
	}

	// If request has already been responded to then send response directly.
	transaction, err := a.transactions.Get(ctx, txid)
	if err != nil {
		return errors.Wrap(err, "get tx")
	}

	if transaction != nil {
		defer a.transactions.Release(ctx, txid)

		contractHash := a.ContractHash()
		allResponded := true
		for _, outputIndex := range requestOutputs {
			transaction.Lock()
			processeds := transaction.ContractProcessed(contractHash, outputIndex)
			transaction.Unlock()
			if len(processeds) == 0 {
				allResponded = false
				continue
			}

			if request.ReplyTo != nil && request.ReplyTo.PeerChannel != nil {
				for _, processed := range processeds {
					if processed.ResponseTxID == nil {
						continue
					}

					responseEtx, err := a.transactions.GetExpandedTx(ctx,
						*processed.ResponseTxID)
					if err != nil {
						return errors.Wrap(err, "get resposne tx")
					}

					if responseEtx == nil {
						return errors.Wrapf(err, "response tx not found: %s",
							*processed.ResponseTxID)
					}

					if err := a.sendPeerChannelResponseTx(ctx, request.ReplyTo.PeerChannel,
						responseEtx); err != nil {
						logger.WarnWithFields(ctx, []logger.Field{
							logger.Stringer("request_txid", txid),
							logger.Stringer("response_txid", *processed.ResponseTxID),
							logger.String("peer_channel",
								request.ReplyTo.PeerChannel.MaskedString()),
						}, "Failed to send peer channel response : %s", err)
					} else {
						logger.InfoWithFields(ctx, []logger.Field{
							logger.Stringer("request_txid", txid),
							logger.Stringer("response_txid", *processed.ResponseTxID),
							logger.String("peer_channel",
								request.ReplyTo.PeerChannel.MaskedString()),
						}, "Posted previous response tx to peer channel")
					}
				}
			}
		}

		if allResponded { // this tx is already fully processed
			if err := a.RemoveResponder(ctx, txid, request.ReplyTo.PeerChannel); err != nil {
				return errors.Wrap(err, "remove responder")
			}

			return nil
		}
	}

	// If the tx is not already known then save it and broadcast it.
	if transaction == nil {
		// Save transaction
		if _, err := a.transactions.AddExpandedTx(ctx, request.Tx); err != nil {
			return errors.Wrap(err, "add expanded tx")
		}
		a.transactions.Release(ctx, txid)

		if err := a.BroadcastTx(ctx, request.Tx, nil); err != nil {
			if _, ok := errors.Cause(err).(spyNodeClient.RejectError); ok {
				if err := a.sendPeerChannelReject(ctx, request.ReplyTo, request.Tx,
					channels.StatusReject, channelsExpandedTx.ProtocolID,
					channelsExpandedTx.ResponseCodeTxRejected, err.Error()); err != nil {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Stringer("request_txid", txid),
						logger.String("peer_channel", request.ReplyTo.PeerChannel.MaskedString()),
					}, "Failed to send peer channel reject response : %s", err)
				}

				return nil
			}

			return errors.Wrap(err, "broadcast")
		}
	}

	return nil
}

func (a *Agent) AddResponder(ctx context.Context, requestTxID bitcoin.Hash32,
	peerChannel *peer_channels.Channel) error {

	newResponder := &state.Responder{
		PeerChannels: peer_channels.Channels{peerChannel},
	}

	agentLockingScript := a.LockingScript()
	responder, err := a.caches.Responders.Add(ctx, agentLockingScript, requestTxID, newResponder)
	if err != nil {
		return errors.Wrap(err, "get responder")
	}

	if responder != newResponder {
		responder.Lock()
		responder.AddPeerChannel(peerChannel)
		responder.Unlock()
	}

	a.caches.Responders.Release(ctx, agentLockingScript, requestTxID)
	return nil
}

func (a *Agent) RemoveResponder(ctx context.Context, requestTxID bitcoin.Hash32,
	peerChannel *peer_channels.Channel) error {

	agentLockingScript := a.LockingScript()
	responder, err := a.caches.Responders.Get(ctx, agentLockingScript, requestTxID)
	if err != nil {
		return errors.Wrap(err, "get responder")
	}

	if responder != nil {
		responder.Lock()
		responder.RemovePeerChannels(peer_channels.Channels{peerChannel})
		responder.Unlock()
		a.caches.Responders.Release(ctx, agentLockingScript, requestTxID)
	}

	return nil
}

func (a *Agent) AddResponse(ctx context.Context, requestTxID bitcoin.Hash32,
	lockingScripts []bitcoin.Script, isContractWide bool, etx *expanded_tx.ExpandedTx) error {

	select {
	case a.peerChannelResponses <- PeerChannelResponse{
		RequestTxID:        requestTxID,
		LockingScripts:     lockingScripts,
		Etx:                etx,
		AgentLockingScript: a.LockingScript(),
	}:
	case <-time.After(a.ChannelTimeout()):
		return errors.Wrap(ErrTimeout, "peer channel response")
	}

	return nil
}

type PeerChannelResponse struct {
	AgentLockingScript bitcoin.Script
	RequestTxID        bitcoin.Hash32
	LockingScripts     []bitcoin.Script
	IsContractWide     bool
	Etx                *expanded_tx.ExpandedTx
}

func ProcessResponses(ctx context.Context, peerChannelResponder *PeerChannelResponder,
	peerChannelResponses chan PeerChannelResponse) error {

	for response := range peerChannelResponses {
		if err := peerChannelResponder.Respond(ctx, response); err != nil {
			return err
		}
	}

	return nil
}

func Respond(ctx context.Context, caches *state.Caches, peerChannelsFactory *peer_channels.Factory,
	response PeerChannelResponse) error {

	if err := postToResponders(ctx, caches, peerChannelsFactory, response.AgentLockingScript,
		response.RequestTxID, response.Etx); err != nil {
		return errors.Wrap(err, "post to responders")
	}

	if len(response.LockingScripts) > 0 {
		if err := postToLockingScriptSubscriptions(ctx, caches, response.AgentLockingScript,
			response.LockingScripts, response.Etx); err != nil {
			return errors.Wrap(err, "post to locking script subscriptions")
		}
	}

	if response.IsContractWide {
		if err := postTransactionToContractSubscriptions(ctx, caches,
			response.AgentLockingScript, response.Etx); err != nil {
			return errors.Wrap(err, "post formation")
		}
	}

	return nil
}

func postToResponders(ctx context.Context, caches *state.Caches,
	peerChannelsFactory *peer_channels.Factory, agentLockingScript bitcoin.Script,
	requestTxID bitcoin.Hash32, etx *expanded_tx.ExpandedTx) error {

	responder, err := caches.Responders.Get(ctx, agentLockingScript, requestTxID)
	if err != nil {
		return errors.Wrap(err, "get responder")
	}

	if responder == nil {
		return nil // no responder
	}
	defer caches.Responders.Release(ctx, agentLockingScript, requestTxID)

	responder.Lock()
	if len(responder.PeerChannels) == 0 {
		responder.Unlock()
		return nil // no peer channels
	}
	cpy := responder.Copy()
	responder.Unlock()

	var toRemove peer_channels.Channels
	for _, peerChannel := range cpy.PeerChannels {
		if err := SendPeerChannelResponseTx(ctx, peerChannelsFactory, peerChannel,
			etx); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("request_txid", requestTxID),
				logger.Stringer("response_txid", etx.TxID()),
				logger.String("peer_channel", peerChannel.MaskedString()),
			}, "Failed to send peer channel response : %s", err)
		} else {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("request_txid", requestTxID),
				logger.Stringer("response_txid", etx.TxID()),
				logger.String("peer_channel", peerChannel.MaskedString()),
			}, "Posted response tx to peer channel")
			toRemove = append(toRemove, peerChannel)
		}
	}

	responder.Lock()
	responder.RemovePeerChannels(toRemove)
	responder.Unlock()

	return nil
}

func (a *Agent) sendPeerChannelResponseTx(ctx context.Context,
	peerChannel *peer_channels.Channel, etx *expanded_tx.ExpandedTx) error {

	return SendPeerChannelResponseTx(ctx, a.peerChannelsFactory, peerChannel, etx)
}

func SendPeerChannelResponseTx(ctx context.Context, peerChannelsFactory *peer_channels.Factory,
	peerChannel *peer_channels.Channel, etx *expanded_tx.ExpandedTx) error {

	if peerChannel == nil {
		return nil
	}

	peerChannelsClient, err := peerChannelsFactory.NewClient(peerChannel.BaseURL)
	if err != nil {
		return errors.Wrapf(err, "peer channel client: %s", peerChannel.BaseURL)
	}

	script, err := client.WrapExpandedTxResponse(etx)
	if err != nil {
		return errors.Wrap(err, "wrap")
	}

	if err := peerChannelsClient.WriteMessage(ctx, peerChannel.ChannelID, peerChannel.Token,
		peer_channels.ContentTypeBinary, bytes.NewReader(script)); err != nil {
		return errors.Wrap(err, "write message")
	}

	return nil
}

func (a *Agent) sendPeerChannelReject(ctx context.Context, replyTo *channels.ReplyTo,
	etx *expanded_tx.ExpandedTx, status channels.Status, rejectProtocolID envelope.ProtocolID,
	rejectCode uint32, message string) error {

	if replyTo == nil || replyTo.PeerChannel == nil {
		return nil
	}

	key := a.Key()
	return SendPeerChannelReject(ctx, a.peerChannelsFactory, replyTo, &key, etx, status,
		rejectProtocolID, rejectCode, message)
}

func SendPeerChannelReject(ctx context.Context, peerChannelsFactory *peer_channels.Factory,
	replyTo *channels.ReplyTo, key *bitcoin.Key, etx *expanded_tx.ExpandedTx,
	status channels.Status, rejectProtocolID envelope.ProtocolID, rejectCode uint32,
	message string) error {

	if replyTo == nil || replyTo.PeerChannel == nil {
		return nil
	}

	peerChannelsClient, err := peerChannelsFactory.NewClient(replyTo.PeerChannel.BaseURL)
	if err != nil {
		return errors.Wrapf(err, "peer channel client: %s", replyTo.PeerChannel.BaseURL)
	}

	response := &channels.Response{
		Status:         status,
		CodeProtocolID: rejectProtocolID,
		Code:           rejectCode,
		Note:           message,
	}

	script, err := client.WrapTxIDResponse(etx.TxID(), response, key)
	if err != nil {
		return errors.Wrap(err, "wrap")
	}

	if err := peerChannelsClient.WriteMessage(ctx, replyTo.PeerChannel.ChannelID,
		replyTo.PeerChannel.Token, peer_channels.ContentTypeBinary,
		bytes.NewReader(script)); err != nil {
		return errors.Wrap(err, "write message")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("request_txid", etx.TxID()),
		logger.String("peer_channel", replyTo.PeerChannel.MaskedString()),
	}, "Posted reject response tx to peer channel")

	return nil
}
