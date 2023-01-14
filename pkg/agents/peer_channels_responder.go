package agents

import (
	"context"

	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/smart_contract_agent/internal/state"
)

type PeerChannelResponder struct {
	caches              *state.Caches
	peerChannelsFactory *peer_channels.Factory
}

func NewPeerChannelResponder(caches *state.Caches,
	peerChannelsFactory *peer_channels.Factory) *PeerChannelResponder {
	return &PeerChannelResponder{
		caches:              caches,
		peerChannelsFactory: peerChannelsFactory,
	}
}

func (r *PeerChannelResponder) Respond(ctx context.Context, response PeerChannelResponse) error {
	return Respond(ctx, r.caches, r.peerChannelsFactory, response)
}
