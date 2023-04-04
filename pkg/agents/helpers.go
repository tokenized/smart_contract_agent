package agents

import (
	"context"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"

	"github.com/pkg/errors"
)

// addResponseInput adds an input to the tx that spends the specified output of the inputTx, unless
// it was already added.
func addResponseInput(tx *txbuilder.TxBuilder, inputTxID bitcoin.Hash32, output *wire.TxOut,
	index int) (uint32, error) {

	for i, txin := range tx.MsgTx.TxIn {
		if txin.PreviousOutPoint.Hash.Equal(&inputTxID) &&
			txin.PreviousOutPoint.Index == uint32(index) {
			return uint32(i), nil // already have this input
		}
	}

	inputIndex := uint32(len(tx.MsgTx.TxIn))
	outpoint := wire.OutPoint{
		Hash:  inputTxID,
		Index: uint32(index),
	}
	if err := tx.AddInput(outpoint, output.LockingScript, output.Value); err != nil {
		return 0, errors.Wrap(err, "add input")
	}

	return inputIndex, nil
}

// addDustLockingScript returns the index of an output with the specified locking script or adds a
// dust output if it doesn't exist.
func addDustLockingScript(tx *txbuilder.TxBuilder, lockingScript bitcoin.Script) (uint32, error) {
	for index, txout := range tx.MsgTx.TxOut {
		if txout.LockingScript.Equal(lockingScript) {
			return uint32(index), nil
		}
	}

	index := uint32(len(tx.MsgTx.TxOut))
	if err := tx.AddOutput(lockingScript, 1, false, true); err != nil {
		return 0, errors.Wrap(err, "add output")
	}

	return index, nil
}

func appendLockingScript(lockingScripts []bitcoin.Script,
	lockingScript bitcoin.Script) []bitcoin.Script {
	for _, ls := range lockingScripts {
		if ls.Equal(lockingScript) {
			return lockingScripts
		}
	}

	return append(lockingScripts, lockingScript)
}

func findBitcoinOutput(tx *wire.MsgTx, lockingScript bitcoin.Script, value uint64) bool {
	for _, txout := range tx.TxOut {
		if txout.LockingScript.Equal(lockingScript) && txout.Value == value {
			return true
		}
	}

	return false
}

func (a *Agent) addResponseTxID(ctx context.Context,
	requestTxID, responseTxID bitcoin.Hash32) (bool, error) {

	requestTransaction, err := a.transactions.Get(ctx, requestTxID)
	if err != nil {
		return false, errors.Wrap(err, "get tx")
	}

	if requestTransaction == nil {
		return false, errors.New("Request transaction not found")
	}
	defer a.transactions.Release(ctx, requestTxID)

	requestTransaction.Lock()

	// Find request action
	config := a.Config()
	requestOutputIndex := -1
	outputCount := requestTransaction.OutputCount()
	for i := 0; i < outputCount; i++ {
		output := requestTransaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, config.IsTest)
		if err != nil {
			continue
		}

		if IsRequest(action) {
			requestOutputIndex = i
			break
		}
	}

	if requestOutputIndex == -1 {
		requestTransaction.Unlock()
		return false, errors.New("Request action not found")
	}

	result := requestTransaction.AddResponseTxID(a.ContractHash(), requestOutputIndex,
		responseTxID)
	requestTransaction.Unlock()

	if result {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("request_txid", requestTxID),
			logger.Int("request_output_index", requestOutputIndex),
			logger.Stringer("response_txid", responseTxID),
		}, "Added response txid")
	}

	if wasRemoved, err := a.removeRecoveryRequest(ctx, requestTxID, requestOutputIndex,
		responseTxID); err != nil {
		return false, errors.Wrap(err, "recovery request")
	} else if wasRemoved {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("request_txid", requestTxID),
			logger.Stringer("response_txid", responseTxID),
			logger.Int("request_output_index", requestOutputIndex),
		}, "Removed recovery request")
	}

	return result, nil
}

func IsRequest(action actions.Action) bool {
	switch action.(type) {
	case *actions.ContractOffer, *actions.ContractAmendment, *actions.ContractAddressChange:
		return true

	case *actions.BodyOfAgreementOffer, *actions.BodyOfAgreementAmendment:
		return true

	case *actions.InstrumentDefinition, *actions.InstrumentModification:
		return true

	case *actions.Transfer:
		return true

	case *actions.Proposal, *actions.BallotCast:
		return true

	case *actions.Order:
		return true

	case *actions.Message:
		return true

	default:
		return false
	}
}

func containsRequest(actions []Action) bool {
	for _, action := range actions {
		if IsRequest(action.Action) {
			return true
		}
	}

	return false
}

func buildExpandedTx(tx *wire.MsgTx, ancestors []*wire.MsgTx) (*expanded_tx.ExpandedTx, error) {
	etx := &expanded_tx.ExpandedTx{
		Tx: tx,
	}

	for _, txin := range tx.TxIn {
		parentTx := etx.Ancestors.GetTx(txin.PreviousOutPoint.Hash)
		if parentTx != nil {
			ptx := parentTx.GetTx()
			if ptx == nil {
				continue // already have this ancestor
			}
		}

		found := false
		for _, ancestor := range ancestors {
			txid := *ancestor.TxHash()
			if txid.Equal(&txin.PreviousOutPoint.Hash) {
				found = true
				etx.Ancestors = append(etx.Ancestors, &expanded_tx.AncestorTx{
					Tx: ancestor,
				})
				break
			}
		}

		if !found {
			return nil, errors.Wrap(expanded_tx.MissingInput,
				"parent tx: "+txin.PreviousOutPoint.Hash.String())
		}
	}

	return etx, nil
}

func getRequestFirstInputLockingScript(ctx context.Context, transactions *transactions.TransactionCache,
	requestTxID bitcoin.Hash32) (bitcoin.Script, error) {

	requestTransaction, err := transactions.Get(ctx, requestTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get request tx")
	}

	if requestTransaction == nil {
		return nil, errors.Wrap(err, "request transaction not found")
	}
	defer transactions.Release(ctx, requestTxID)

	requestTransaction.Lock()
	defer requestTransaction.Unlock()

	requestInputOutput, err := requestTransaction.InputOutput(0)
	if err != nil {
		return nil, errors.Wrapf(err, "request input locking script %d", 0)
	}

	return requestInputOutput.LockingScript, nil
}
