package state

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/tokenized/pkg/bitcoin"
)

var (
	endian = binary.LittleEndian
)

func LockingScriptHash(lockingScript bitcoin.Script) bitcoin.Hash32 {
	return bitcoin.Hash32(sha256.Sum256(lockingScript))
}

func appendHashIfDoesntExist(list []bitcoin.Hash32, value bitcoin.Hash32) []bitcoin.Hash32 {
	for _, v := range list {
		if v.Equal(&value) {
			return list // already contains value
		}
	}

	return append(list, value) // add value
}
