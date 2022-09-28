package agents

import (
	"context"

	"github.com/tokenized/smart_contract_agent/internal/state"

	"github.com/pkg/errors"
)

// sendReject creates a reject message transaction that spends the specified output and contains the
// specified reject code.
func (a *Agent) sendReject(ctx context.Context, transaction *state.Transaction, responseOutput int,
	rejectCode int, message string) error {

	// rejection := &actions.Rejection{
	// 	// AddressIndexes       []uint32 `protobuf:"varint,1,rep,packed,name=AddressIndexes,proto3" json:"AddressIndexes,omitempty"`
	// 	// RejectAddressIndex   uint32   `protobuf:"varint,2,opt,name=RejectAddressIndex,proto3" json:"RejectAddressIndex,omitempty"`
	// 	RejectionCode: uint32(rejectCode),
	// 	Message: message,
	// 	Timestamp: uint64(time.Now().UnixNano()),
	// }

	return errors.New("Not Implemented")
}
