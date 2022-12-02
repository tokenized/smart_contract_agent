package agents

import (
	"bytes"
	"context"

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

		if request.ReplyTo != nil && request.ReplyTo.PeerChannel != nil {
			if err := a.sendPeerChannelReject(ctx, request.ReplyTo.PeerChannel, request.Tx,
				channels.StatusReject, channelsExpandedTx.ProtocolID,
				channelsExpandedTx.ResponseCodeMissingAncestors, err.Error()); err != nil {
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("request_txid", txid),
					logger.String("peer_channel", request.ReplyTo.PeerChannel.URL),
				}, "Failed to send peer channel reject response : %s", err)
			} else {
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Stringer("request_txid", txid),
					logger.String("peer_channel", request.ReplyTo.PeerChannel.URL),
				}, "Posted reject response tx to peer channel")
			}
		}

		return nil
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("request_txid", txid),
	}, "Received peer channel message with transaction")

	agentLockingScript := a.LockingScript()
	isTest := a.IsTest()

	requestOutputs, err := relevantRequestOutputs(request.Tx, agentLockingScript, isTest)
	if err != nil {
		return errors.Wrap(err, "tx is relevant")
	}

	if len(requestOutputs) == 0 {
		logger.Warn(ctx, "Transaction is not relevant")

		if request.ReplyTo != nil && request.ReplyTo.PeerChannel != nil {
			if err := a.sendPeerChannelReject(ctx, request.ReplyTo.PeerChannel, request.Tx,
				channels.StatusReject, client.ProtocolID, client.ResponseCodeNotRelevant,
				""); err != nil {
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("request_txid", txid),
					logger.String("peer_channel", request.ReplyTo.PeerChannel.URL),
				}, "Failed to send peer channel reject response : %s", err)
			} else {
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Stringer("request_txid", txid),
					logger.String("peer_channel", request.ReplyTo.PeerChannel.URL),
				}, "Posted reject response tx to peer channel")
			}
		}

		return nil
	}

	if request.ReplyTo != nil && request.ReplyTo.PeerChannel != nil {
		if err := a.AddResponder(ctx, txid, request.ReplyTo.PeerChannel); err != nil {
			return errors.Wrap(err, "add responder")
		}
	}

	// If request has already been responded to then send response directly.
	transaction, err := a.caches.Transactions.Get(ctx, txid)
	if err != nil {
		return errors.Wrap(err, "get tx")
	}

	if transaction != nil {
		defer a.caches.Transactions.Release(ctx, txid)

		contractHash := a.ContractHash()
		allResponded := true
		transaction.Lock()
		for _, outputIndex := range requestOutputs {
			processeds := transaction.ContractProcessed(contractHash, outputIndex)
			if len(processeds) == 0 {
				allResponded = false
				continue
			}

			if request.ReplyTo != nil && request.ReplyTo.PeerChannel != nil {
				for _, processed := range processeds {
					if processed.ResponseTxID == nil {
						continue
					}

					responseEtx, err := a.caches.Transactions.GetExpandedTx(ctx,
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
							logger.String("peer_channel", request.ReplyTo.PeerChannel.URL),
						}, "Failed to send peer channel response : %s", err)
					} else {
						logger.InfoWithFields(ctx, []logger.Field{
							logger.Stringer("request_txid", txid),
							logger.Stringer("response_txid", *processed.ResponseTxID),
							logger.String("peer_channel", request.ReplyTo.PeerChannel.URL),
						}, "Posted previous response tx to peer channel")
					}
				}
			}
		}
		transaction.Unlock()

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
		if _, err := a.caches.Transactions.AddExpandedTx(ctx, request.Tx); err != nil {
			return errors.Wrap(err, "add expanded tx")
		}
		a.caches.Transactions.Release(ctx, txid)

		if err := a.BroadcastTx(ctx, request.Tx, nil); err != nil {
			if _, ok := errors.Cause(err).(spyNodeClient.RejectError); ok {
				if request.ReplyTo != nil && request.ReplyTo.PeerChannel != nil {
					if err := a.sendPeerChannelReject(ctx, request.ReplyTo.PeerChannel, request.Tx,
						channels.StatusReject, channelsExpandedTx.ProtocolID,
						channelsExpandedTx.ResponseCodeTxRejected, err.Error()); err != nil {
						logger.WarnWithFields(ctx, []logger.Field{
							logger.Stringer("request_txid", txid),
							logger.String("peer_channel", request.ReplyTo.PeerChannel.URL),
						}, "Failed to send peer channel reject response : %s", err)
					} else {
						logger.InfoWithFields(ctx, []logger.Field{
							logger.Stringer("request_txid", txid),
							logger.String("peer_channel", request.ReplyTo.PeerChannel.URL),
						}, "Posted reject response tx to peer channel")
					}
				}

				return nil
			}

			return errors.Wrap(err, "broadcast")
		}
	}

	return nil
}

func (a *Agent) AddResponder(ctx context.Context, requestTxID bitcoin.Hash32,
	peerChannel *peer_channels.PeerChannel) error {

	newResponder := &state.Responder{
		PeerChannels: peer_channels.PeerChannels{peerChannel},
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
	peerChannel *peer_channels.PeerChannel) error {

	agentLockingScript := a.LockingScript()
	responder, err := a.caches.Responders.Get(ctx, agentLockingScript, requestTxID)
	if err != nil {
		return errors.Wrap(err, "get responder")
	}

	if responder != nil {
		responder.Lock()
		responder.RemovePeerChannels(peer_channels.PeerChannels{peerChannel})
		responder.Unlock()
		a.caches.Responders.Release(ctx, agentLockingScript, requestTxID)
	}

	return nil
}

func (a *Agent) Respond(ctx context.Context, requestTxID bitcoin.Hash32,
	responseTransaction *state.Transaction) error {

	agentLockingScript := a.LockingScript()
	responder, err := a.caches.Responders.Get(ctx, agentLockingScript, requestTxID)
	if err != nil {
		return errors.Wrap(err, "get responder")
	}

	if responder == nil {
		return nil // no responder
	}
	defer a.caches.Responders.Release(ctx, agentLockingScript, requestTxID)

	responder.Lock()
	if len(responder.PeerChannels) == 0 {
		responder.Unlock()
		return nil // no peer channels
	}
	cpy := responder.Copy()
	responder.Unlock()

	responseTransaction.Lock()
	responseEtx, err := a.caches.Transactions.ExpandedTx(ctx, responseTransaction)
	responseTransaction.Unlock()
	if err != nil {
		return errors.Wrap(err, "expanded tx")
	}

	var toRemove peer_channels.PeerChannels
	for _, peerChannel := range cpy.PeerChannels {
		if err := a.sendPeerChannelResponseTx(ctx, peerChannel, responseEtx); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("request_txid", requestTxID),
				logger.Stringer("response_txid", responseEtx.TxID()),
				logger.String("peer_channel", peerChannel.URL),
			}, "Failed to send peer channel response : %s", err)
		} else {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("request_txid", requestTxID),
				logger.Stringer("response_txid", responseEtx.TxID()),
				logger.String("peer_channel", peerChannel.URL),
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
	peerChannel *peer_channels.PeerChannel, etx *expanded_tx.ExpandedTx) error {

	if peerChannel == nil {
		return nil
	}

	baseURL, channelID, err := peer_channels.ParseChannelURL(peerChannel.URL)
	if err != nil {
		return errors.Wrap(err, "url")
	}

	peerChannelsClient, err := a.peerChannelsFactory.NewClient(baseURL)
	if err != nil {
		return errors.Wrapf(err, "peer channel client: %s", baseURL)
	}

	script, err := client.WrapExpandedTxResponse(etx)
	if err != nil {
		return errors.Wrap(err, "wrap")
	}

	if err := peerChannelsClient.WriteMessage(ctx, channelID, peerChannel.Token,
		peer_channels.ContentTypeBinary, bytes.NewReader(script)); err != nil {
		return errors.Wrap(err, "write message")
	}

	return nil
}

func (a *Agent) sendPeerChannelReject(ctx context.Context, peerChannel *peer_channels.PeerChannel,
	etx *expanded_tx.ExpandedTx, status channels.Status, rejectProtocolID envelope.ProtocolID,
	rejectCode uint32, message string) error {

	if peerChannel == nil {
		return nil
	}

	baseURL, channelID, err := peer_channels.ParseChannelURL(peerChannel.URL)
	if err != nil {
		return errors.Wrap(err, "url")
	}

	peerChannelsClient, err := a.peerChannelsFactory.NewClient(baseURL)
	if err != nil {
		return errors.Wrapf(err, "peer channel client: %s", baseURL)
	}

	response := &channels.Response{
		Status:         status,
		CodeProtocolID: rejectProtocolID,
		Code:           rejectCode,
		Note:           message,
	}

	key := a.Key()
	script, err := client.WrapTxIDResponse(etx.TxID(), response, &key)
	if err != nil {
		return errors.Wrap(err, "wrap")
	}

	if err := peerChannelsClient.WriteMessage(ctx, channelID, peerChannel.Token,
		peer_channels.ContentTypeBinary, bytes.NewReader(script)); err != nil {
		return errors.Wrap(err, "write message")
	}

	return nil
}
