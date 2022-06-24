package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/tokenized/channels"
	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

var (
	ErrNotRelevant = errors.New("Not Relevant")
)

type Agent struct {
	key            bitcoin.Key
	derivationHash bitcoin.Hash32

	lockingScript bitcoin.Script
	isTest        bool

	pendingTxLock sync.Mutex
	pendingTxs    map[bitcoin.Hash32]*channels.ExpandedTx

	lock sync.Mutex
}

func NewAgent(baseKey bitcoin.Key, derivationHash bitcoin.Hash32) (*Agent, error) {
	result := &Agent{
		derivationHash: derivationHash,
		pendingTxs:     make(map[bitcoin.Hash32]*channels.ExpandedTx),
	}

	key, err := baseKey.AddHash(derivationHash)
	if err != nil {
		return nil, errors.Wrap(err, "derive key")
	}
	result.key = key

	return result, nil
}

func (a *Agent) LockingScript() bitcoin.Script {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.lockingScript
}

func (a *Agent) IsTest() bool {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.isTest
}

func (a *Agent) ActionIsSupported(action actions.Action) bool {
	switch action.(type) {
	case *actions.ContractOffer, *actions.ContractFormation, *actions.ContractAmendment,
		*actions.ContractAddressChange:
		return true

	case *actions.BodyOfAgreementOffer, *actions.BodyOfAgreementFormation,
		*actions.BodyOfAgreementAmendment:
		return true

	case *actions.InstrumentDefinition, *actions.InstrumentCreation,
		*actions.InstrumentModification:
		return true

	case *actions.Transfer, *actions.Settlement:
		return true

	case *actions.Proposal, *actions.Vote, *actions.BallotCast, *actions.BallotCounted,
		*actions.Result:
		return true

	case *actions.Order, *actions.Freeze, *actions.Thaw, *actions.Confiscation,
		*actions.Reconciliation:
		return true

	case *actions.Message, *actions.Rejection:
		return true

	default:
		return false
	}
}

func (a *Agent) Formation(ctx context.Context) (*actions.ContractFormation, error) {
	return nil, errors.New("Not Implemented")
}

func (a *Agent) AddTx(ctx context.Context, contextID bitcoin.Hash32,
	etx *channels.ExpandedTx) error {

	lockingScript := a.LockingScript()
	isRelevant := false

	for i := range etx.Tx.TxIn {
		inputLockingScript, err := etx.InputLockingScript(i)
		if err != nil {
			return errors.Wrapf(err, "input locking script %d", i)
		}

		if inputLockingScript.Equal(lockingScript) {
			isRelevant = true
		}
	}

	isTest := a.IsTest()
	hasAction := false
	for _, txout := range etx.Tx.TxOut {
		if txout.LockingScript.Equal(lockingScript) {
			isRelevant = true
		}

		action, err := protocol.Deserialize(txout.LockingScript, isTest)
		if err != nil {
			continue
		}

		if a.ActionIsSupported(action) {
			hasAction = true
		}
	}

	if !isRelevant {
		return ErrNotRelevant
	}

	if !hasAction {
		return nil
	}

	a.pendingTxLock.Lock()
	a.pendingTxs[*etx.Tx.TxHash()] = etx
	a.pendingTxLock.Unlock()

	return nil
}

func (a *Agent) UpdateTransaction(ctx context.Context, txid bitcoin.Hash32,
	state wallet.TxState) error {

	if state == wallet.TxStateSafe {
		if err := a.applyTx(ctx, txid); err != nil {
			return errors.Wrap(err, "apply tx")
		}
	}

	return nil
}

func (a *Agent) applyTx(ctx context.Context, txid bitcoin.Hash32) error {
	// Check if tx has already been applied.

	// Perform action and send response tx to chain and to relevant subscribed Channels.

	return errors.New("Not Implemented")
}

func (a *Agent) applyAction(ctx context.Context, action actions.Action,
	tx *state.Transaction) error {

	switch action.(type) {
	case *actions.ContractOffer:
	case *actions.ContractFormation:
	case *actions.ContractAmendment:
	case *actions.ContractAddressChange:
	case *actions.BodyOfAgreementOffer:
	case *actions.BodyOfAgreementFormation:
	case *actions.BodyOfAgreementAmendment:
	case *actions.InstrumentDefinition:
	case *actions.InstrumentCreation:
	case *actions.InstrumentModification:
	case *actions.Transfer:
	case *actions.Settlement:
	case *actions.Proposal:
	case *actions.Vote:
	case *actions.BallotCast:
	case *actions.BallotCounted:
	case *actions.Result:
	case *actions.Order:
	case *actions.Freeze:
	case *actions.Thaw:
	case *actions.Confiscation:
	case *actions.Reconciliation:
	case *actions.Message:
	case *actions.Rejection:

	default:
		return fmt.Errorf("Action %s not supported", action.Code())
	}

	return nil
}
