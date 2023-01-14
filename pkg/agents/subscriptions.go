package agents

import (
	"context"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/smart_contract_agent/internal/state"
)

// postToLockingScriptSubscriptions posts the transaction to any subscriptions for the relevant
// locking scripts.
func postToLockingScriptSubscriptions(ctx context.Context, caches *state.Caches,
	agentLockingScript bitcoin.Script, lockingScripts []bitcoin.Script,
	etx *expanded_tx.ExpandedTx) error {

	// subscriptions, err := caches.Subscriptions.GetLockingScriptMulti(ctx, agentLockingScript,
	// 	lockingScripts)
	// if err != nil {
	// 	return errors.Wrap(err, "get subscriptions")
	// }
	// defer caches.Subscriptions.ReleaseMulti(ctx, agentLockingScript, subscriptions)

	// if len(subscriptions) == 0 {
	// 	return nil
	// }

	// msg := channels_expanded_tx.ExpandedTxMessage(*etx)

	// for _, subscription := range subscriptions {
	// 	if subscription == nil {
	// 		continue
	// 	}

	// 	subscription.Lock()
	// 	channelHash := subscription.GetChannelHash()
	// 	subscription.Unlock()

	// 	// Send settlement over channel
	// 	channel, err := a.GetChannel(ctx, channelHash)
	// 	if err != nil {
	// 		return errors.Wrapf(err, "get channel : %s", channelHash)
	// 	}
	// 	if channel == nil {
	// 		continue
	// 	}

	// 	if err := channel.SendMessage(ctx, &msg); err != nil {
	// 		logger.WarnWithFields(ctx, []logger.Field{
	// 			logger.Stringer("channel", channelHash),
	// 		}, "Failed to send channels message : %s", err)
	// 	}
	// }

	return nil
}

// postTransactionToContractSubscriptions posts the transaction to any subscriptions for the
// relevant locking scripts for the contract.
func postTransactionToContractSubscriptions(ctx context.Context, caches *state.Caches,
	agentLockingScript bitcoin.Script, etx *expanded_tx.ExpandedTx) error {

	// subscriptions, err := a.caches.Subscriptions.GetLockingScriptMulti(ctx, agentLockingScript,
	// 	lockingScripts)
	// if err != nil {
	// 	return errors.Wrap(err, "get subscriptions")
	// }
	// defer a.caches.Subscriptions.ReleaseMulti(ctx, agentLockingScript, subscriptions)

	// if len(subscriptions) == 0 {
	// 	return nil
	// }

	// expandedTx, err := transaction.ExpandedTx(ctx)
	// if err != nil {
	// 	return errors.Wrap(err, "get expanded tx")
	// }

	// msg := channels_expanded_tx.ExpandedTxMessage(*expandedTx)

	// for _, subscription := range subscriptions {
	// 	if subscription == nil {
	// 		continue
	// 	}

	// 	subscription.Lock()
	// 	channelHash := subscription.GetChannelHash()
	// 	subscription.Unlock()

	// 	// Send settlement over channel
	// 	channel, err := a.GetChannel(ctx, channelHash)
	// 	if err != nil {
	// 		return errors.Wrapf(err, "get channel : %s", channelHash)
	// 	}
	// 	if channel == nil {
	// 		continue
	// 	}

	// 	if err := channel.SendMessage(ctx, &msg); err != nil {
	// 		logger.WarnWithFields(ctx, []logger.Field{
	// 			logger.Stringer("channel", channelHash),
	// 		}, "Failed to send channels message : %s", err)
	// 	}
	// }

	return nil
}
