package commands

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/txbuilder"

	"github.com/pkg/errors"
)

const (
	DefaultFeeRate     = 0.05
	DefaultDustFeeRate = 0.0
)

func FeeRate() (float64, error) {
	feeRateString := os.Getenv("FEE_RATE")
	if len(feeRateString) == 0 {
		fmt.Printf("Using %.4f fee rate (satoshis per byte) (set with FEE_RATE env value)\n",
			DefaultFeeRate)
		return DefaultFeeRate, nil // use default value
	}

	feeRateValue, err := strconv.ParseFloat(feeRateString, 64)
	if err != nil {
		return 0.0, errors.Wrap(err, "FEE_RATE environment value")
	}

	fmt.Printf("Using %.4f fee rate (satoshis per byte) (set with FEE_RATE env value)\n", feeRateValue)
	return feeRateValue, nil
}

func DustFeeRate() (float64, error) {
	feeRateString := os.Getenv("DUST_FEE_RATE")
	if len(feeRateString) == 0 {
		fmt.Printf("Using %.4f dust fee rate (satoshis per byte) (set with DUST_FEE_RATE env value)\n",
			DefaultDustFeeRate)
		return DefaultDustFeeRate, nil // use default value
	}

	feeRateValue, err := strconv.ParseFloat(feeRateString, 64)
	if err != nil {
		return 0.0, errors.Wrap(err, "DUST_FEE_RATE environment value")
	}

	fmt.Printf("Using %.4f dust fee rate (satoshis per byte) (set with DUST_FEE_RATE env value)\n",
		feeRateValue)
	return feeRateValue, nil
}

func NewTxBuilder() (*txbuilder.TxBuilder, error) {
	feeRate, err := FeeRate()
	if err != nil {
		return nil, errors.Wrap(err, "fee rate")
	}

	dustFeeRate, err := DustFeeRate()
	if err != nil {
		return nil, errors.Wrap(err, "dust fee rate")
	}

	return txbuilder.NewTxBuilder(float32(feeRate), float32(dustFeeRate)), nil
}

func IsTest() (bool, error) {
	isTest := true
	isTestString := os.Getenv("IS_TEST")
	if len(isTestString) > 0 {
		switch isTestString {
		case "true", "TRUE":
			isTest = true
		case "false", "FALSE":
			isTest = false
		default:
			return false,
				fmt.Errorf("Invalid IS_TEST environment value (must be \"true\" or \"false\"): %s",
					isTestString)
		}
	}

	if isTest {
		fmt.Printf("Using Test Tokenized Protocol ID (set with IS_TEST env value)\n")
	} else {
		fmt.Printf("Using Production Tokenized Protocol ID (set with IS_TEST env value)\n")
	}

	return isTest, nil
}

func Network() (bitcoin.Network, error) {
	network := bitcoin.MainNet
	networkString := os.Getenv("BITCOIN_NETWORK")
	if len(networkString) > 0 {
		network = bitcoin.NetworkFromString(networkString)
	}

	fmt.Printf("Using %s Network (set with BITCOIN_NETWORK env value)\n", network)
	return network, nil
}
