package state

import (
	"encoding/binary"
	"io"

	"github.com/tokenized/pkg/bitcoin"

	"github.com/pkg/errors"
)

var (
	endian = binary.LittleEndian
)

func writeString(w io.Writer, b []byte) error {
	if err := binary.Write(w, endian, uint32(len(b))); err != nil {
		return errors.Wrap(err, "size")
	}

	if _, err := w.Write(b); err != nil {
		return errors.Wrap(err, "value")
	}

	return nil
}

func readString(r io.Reader) ([]byte, error) {
	var s uint32
	if err := binary.Read(r, endian, &s); err != nil {
		return nil, errors.Wrap(err, "size")
	}

	b := make([]byte, s)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, errors.Wrap(err, "value")
	}

	return b, nil
}

func hashPathID(hash bitcoin.Hash32) [2]byte {
	var pathID [2]byte
	copy(pathID[:], hash[:])
	return pathID
}

func appendStringIfDoesntExist(list []string, value string) []string {
	for _, v := range list {
		if v == value {
			return list // already contains value
		}
	}

	return append(list, value) // add value
}

func appendScriptIfDoesntExist(list []bitcoin.Script, value bitcoin.Script) []bitcoin.Script {
	for _, v := range list {
		if v.Equal(value) {
			return list // already contains value
		}
	}

	return append(list, value) // add value
}
