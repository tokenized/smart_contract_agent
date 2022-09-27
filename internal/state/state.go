package state

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"

	"github.com/tokenized/pkg/bitcoin"
)

var (
	endian = binary.LittleEndian

	isTestLock sync.Mutex
	isTest     = true
)

type ContractID bitcoin.Hash32

type InstrumentCode bitcoin.Hash20

func SetIsTest(value bool) {
	isTestLock.Lock()
	isTest = value
	isTestLock.Unlock()
}

func IsTest() bool {
	isTestLock.Lock()
	value := isTest
	isTestLock.Unlock()
	return value
}

func CalculateContractHash(lockingScript bitcoin.Script) ContractID {
	return ContractID(sha256.Sum256(lockingScript))
}

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
