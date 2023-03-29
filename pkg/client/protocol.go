package client

import (
	"github.com/tokenized/channels"
	envelope "github.com/tokenized/envelope/pkg/golang/envelope/base"
)

const (
	ResponseCodeNotRelevant = uint32(1)
)

var (
	ProtocolID = envelope.ProtocolID("TKN-SCA") // Protocol ID for Tokenized Smart Contract Agent
)

// Protocol is only functional to parse the response code above since there are no protocol specific
// message structures.
type Protocol struct{}

func NewProtocol() *Protocol {
	return &Protocol{}
}

func (*Protocol) ProtocolID() envelope.ProtocolID {
	return ProtocolID
}

func (*Protocol) Parse(payload envelope.Data) (channels.Message, error) {
	return Parse(payload)
}

func (*Protocol) ResponseCodeToString(code uint32) string {
	return ResponseCodeToString(code)
}

func Parse(payload envelope.Data) (channels.Message, error) {
	return nil, channels.ErrUnsupportedProtocol
}

func ResponseCodeToString(code uint32) string {
	switch code {
	case ResponseCodeNotRelevant:
		return "not_relevant"
	default:
		return "parse_error"
	}
}
