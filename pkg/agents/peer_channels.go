package agents

import (
	"bytes"
	"context"

	"github.com/tokenized/channels"
	channelsExpandedTx "github.com/tokenized/channels/expanded_tx"
	channelsWallet "github.com/tokenized/channels/wallet"
	envelope "github.com/tokenized/envelope/pkg/golang/envelope/base"
	envelopeV1 "github.com/tokenized/envelope/pkg/golang/envelope/v1"
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

	payload, err := envelopeV1.Parse(bytes.NewReader(msg.Payload))
	if err != nil {
		return errors.Wrap(err, "parse envelope")
	}

	var replyTo *channels.ReplyTo
	replyTo, payload, err = channels.ParseReplyTo(payload)
	if err != nil {
		return errors.Wrap(err, "parse reply to")
	}

	if len(payload.ProtocolIDs) == 0 {
		return errors.Wrap(channels.ErrUnsupportedProtocol, "no data protocol")
	}

	if len(payload.ProtocolIDs) > 1 {
		return errors.Wrap(channels.ErrUnsupportedProtocol, "more than one data protocol")
	}

	protocol := a.peerChannelsProtocols.GetProtocol(payload.ProtocolIDs[0])
	if protocol == nil {
		return errors.Wrap(channels.ErrUnsupportedProtocol, "unsupported data protocol")
	}

	data, err := protocol.Parse(payload)
	if err != nil {
		return errors.Wrap(err, "parse data")
	}

	cetx, ok := data.(*channelsExpandedTx.ExpandedTxMessage)
	if !ok {
		return errors.Wrap(channels.ErrUnsupportedProtocol, "wrong data protocol")
	}

	etxp := expanded_tx.ExpandedTx(*cetx)
	etx := &etxp
	txid := etx.TxID()

	// Verify that expanded tx inputs were provided.
	if err := etx.VerifyAncestors(); err != nil {
		if replyTo != nil && replyTo.PeerChannel != nil {
			if err := a.sendPeerChannelReject(ctx, replyTo.PeerChannel, etx,
				channels.StatusReject, channelsExpandedTx.ProtocolID,
				channelsExpandedTx.ResponseCodeMissingAncestors, err.Error()); err != nil {
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("request_txid", txid),
					logger.String("peer_channel", replyTo.PeerChannel.URL),
				}, "Failed to send peer channel reject response : %s", err)
			} else {
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Stringer("request_txid", txid),
					logger.String("peer_channel", replyTo.PeerChannel.URL),
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

	requestOutputs, err := relevantRequestOutputs(etx, agentLockingScript, isTest)
	if err != nil {
		return errors.Wrap(err, "tx is relevant")
	}

	if len(requestOutputs) == 0 {
		logger.Warn(ctx, "Transaction is not relevant")

		if replyTo != nil && replyTo.PeerChannel != nil {
			if err := a.sendPeerChannelReject(ctx, replyTo.PeerChannel, etx, channels.StatusReject,
				client.ProtocolID, client.ResponseCodeNotRelevant, ""); err != nil {
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("request_txid", txid),
					logger.String("peer_channel", replyTo.PeerChannel.URL),
				}, "Failed to send peer channel reject response : %s", err)
			} else {
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Stringer("request_txid", txid),
					logger.String("peer_channel", replyTo.PeerChannel.URL),
				}, "Posted reject response tx to peer channel")
			}
		}

		return nil
	}

	if replyTo != nil && replyTo.PeerChannel != nil {
		if err := a.AddResponder(ctx, txid, replyTo.PeerChannel); err != nil {
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

			if replyTo != nil && replyTo.PeerChannel != nil {
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
						return errors.Wrapf(err, "response tx not found: %s", *processed.ResponseTxID)
					}

					if err := a.sendPeerChannelResponseTx(ctx, replyTo.PeerChannel,
						responseEtx); err != nil {
						logger.WarnWithFields(ctx, []logger.Field{
							logger.Stringer("request_txid", txid),
							logger.Stringer("response_txid", *processed.ResponseTxID),
							logger.String("peer_channel", replyTo.PeerChannel.URL),
						}, "Failed to send peer channel response : %s", err)
					} else {
						logger.InfoWithFields(ctx, []logger.Field{
							logger.Stringer("request_txid", txid),
							logger.Stringer("response_txid", *processed.ResponseTxID),
							logger.String("peer_channel", replyTo.PeerChannel.URL),
						}, "Posted previous response tx to peer channel")
					}
				}
			}
		}
		transaction.Unlock()

		if allResponded { // this tx is already fully processed
			if err := a.RemoveResponder(ctx, txid, replyTo.PeerChannel); err != nil {
				return errors.Wrap(err, "remove responder")
			}

			return nil
		}
	}

	// If the tx is not already known then save it and broadcast it.
	if transaction == nil {
		// Save transaction
		if _, err := a.caches.Transactions.AddExpandedTx(ctx, etx); err != nil {
			return errors.Wrap(err, "add expanded tx")
		}
		a.caches.Transactions.Release(ctx, txid)

		if err := a.BroadcastTx(ctx, etx, nil); err != nil {
			if _, ok := errors.Cause(err).(spyNodeClient.RejectError); ok {
				if replyTo != nil && replyTo.PeerChannel != nil {
					if err := a.sendPeerChannelReject(ctx, replyTo.PeerChannel, etx,
						channels.StatusReject, channelsExpandedTx.ProtocolID,
						channelsExpandedTx.ResponseCodeTxRejected, err.Error()); err != nil {
						logger.WarnWithFields(ctx, []logger.Field{
							logger.Stringer("request_txid", txid),
							logger.String("peer_channel", replyTo.PeerChannel.URL),
						}, "Failed to send peer channel reject response : %s", err)
					} else {
						logger.InfoWithFields(ctx, []logger.Field{
							logger.Stringer("request_txid", txid),
							logger.String("peer_channel", replyTo.PeerChannel.URL),
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
	peerChannel *peer_channels.PeerChannel, responseEtx *expanded_tx.ExpandedTx) error {

	if peerChannel == nil {
		return nil
	}

	baseURL, channelID, err := peer_channels.ParseChannelURL(peerChannel.URL)
	if err != nil {
		return errors.Wrap(err, "url")
	}

	client, err := a.peerChannelsFactory.NewClient(baseURL)
	if err != nil {
		return errors.Wrapf(err, "peer channel client: %s", baseURL)
	}

	cetx := channelsExpandedTx.ExpandedTxMessage(*responseEtx)

	payload, err := cetx.Write()
	if err != nil {
		return errors.Wrap(err, "payload")
	}

	scriptItems := envelopeV1.Wrap(payload)
	script, err := scriptItems.Script()
	if err != nil {
		return errors.Wrap(err, "script")
	}

	if err := client.WriteMessage(ctx, channelID, peerChannel.Token,
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

	cetx := channelsExpandedTx.ExpandedTxMessage(*etx)

	payload, err := cetx.Write()
	if err != nil {
		return errors.Wrap(err, "write")
	}

	response := &channels.Response{
		Status:         status,
		CodeProtocolID: rejectProtocolID,
		Code:           rejectCode,
		Note:           message,
	}

	payload, err = response.Wrap(payload)
	if err != nil {
		return errors.Wrap(err, "response")
	}

	hash := channelsWallet.RandomHash()
	payload, err = channels.WrapSignature(payload, a.Key(), &hash, false)
	if err != nil {
		return errors.Wrap(err, "sign")
	}

	scriptItems := envelopeV1.Wrap(payload)
	script, err := scriptItems.Script()
	if err != nil {
		return errors.Wrap(err, "script")
	}

	if err := peerChannelsClient.WriteMessage(ctx, channelID, peerChannel.Token,
		peer_channels.ContentTypeBinary, bytes.NewReader(script)); err != nil {
		return errors.Wrap(err, "write message")
	}

	return nil
}
