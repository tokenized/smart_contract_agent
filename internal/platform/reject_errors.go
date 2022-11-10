package platform

import (
	"fmt"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/specification/dist/golang/actions"
)

type RejectError struct {
	Code                  uint32
	Message               string
	InputIndex            int
	OutputIndex           int
	ReceiverLockingScript bitcoin.Script
}

func NewRejectError(code int, message string) RejectError {
	return RejectError{
		Code:        uint32(code),
		Message:     message,
		InputIndex:  0,
		OutputIndex: -1,
	}
}

func NewRejectErrorWithOutputIndex(code int, message string, outputIndex int) RejectError {
	return RejectError{
		Code:        uint32(code),
		Message:     message,
		InputIndex:  0,
		OutputIndex: outputIndex,
	}
}

func NewRejectErrorFull(code int, message string, inputIndex, outputIndex int,
	receiverLockingScript bitcoin.Script) RejectError {

	return RejectError{
		Code:                  uint32(code),
		Message:               message,
		InputIndex:            inputIndex,
		OutputIndex:           outputIndex,
		ReceiverLockingScript: receiverLockingScript,
	}
}

func (e RejectError) Label() string {
	rejectData := actions.RejectionsData(e.Code)
	if rejectData != nil {
		return rejectData.Label
	}

	return "<unknown>"
}

func (e RejectError) Error() string {
	codeLabel := "<unknown>"
	rejectData := actions.RejectionsData(e.Code)
	if rejectData != nil {
		codeLabel = rejectData.Label
	}

	result := fmt.Sprintf("Reject (%d): %s", e.Code, codeLabel)
	if len(e.Message) > 0 {
		result += ": " + e.Message
	}

	return result
}
