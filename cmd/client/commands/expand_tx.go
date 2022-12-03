package commands

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	channelsExpandedTx "github.com/tokenized/channels/expanded_tx"
	envelopeV1 "github.com/tokenized/envelope/pkg/golang/envelope/v1"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var ExpandTx = &cobra.Command{
	Use:   "expand_tx tx_hex",
	Short: "Converts a hex tx into an expanded tx.",
	Args:  cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		ctx := logger.ContextWithLogger(context.Background(), true, true, "")

		b, err := hex.DecodeString(args[0])
		if err != nil {
			return errors.Wrap(err, "hex")
		}

		tx := &wire.MsgTx{}
		if err := tx.Deserialize(bytes.NewReader(b)); err != nil {
			return errors.Wrap(err, "tx")
		}

		etx := &expanded_tx.ExpandedTx{
			Tx: tx,
		}

		for i, txin := range tx.TxIn {
			if etx.Ancestors.GetTx(txin.PreviousOutPoint.Hash) != nil {
				continue
			}

			atx, err := getTx(ctx, txin.PreviousOutPoint.Hash)
			if err != nil {
				return errors.Wrapf(err, "get input tx %d: %s", i, txin.PreviousOutPoint.Hash)
			}

			etx.Ancestors = append(etx.Ancestors, &expanded_tx.AncestorTx{
				Tx: atx,
			})
		}

		script, err := wrapExpandedTx(etx)
		if err != nil {
			return errors.Wrap(err, "wrap")
		}

		fmt.Printf("Expanded Tx : %x\n", []byte(script))

		return nil
	},
}

func wrapExpandedTx(etx *expanded_tx.ExpandedTx) (bitcoin.Script, error) {
	cetx := channelsExpandedTx.ExpandedTxMessage(*etx)

	payload, err := cetx.Write()
	if err != nil {
		return nil, errors.Wrap(err, "write")
	}

	return envelopeV1.Wrap(payload).Script()
}

func getTx(ctx context.Context, txid bitcoin.Hash32) (*wire.MsgTx, error) {
	const URLGetRawTx = "https://api.whatsonchain.com/v1/bsv/%s/tx/%s/hex"
	url := fmt.Sprintf(URLGetRawTx, "main", txid)

	apiKey := ""
	var response string
	if err := getWithToken(ctx, url, apiKey, &response); err != nil {
		return nil, errors.Wrap(err, "get")
	}

	b, err := hex.DecodeString(response)
	if err != nil {
		return nil, errors.Wrap(err, "hex")
	}

	tx := &wire.MsgTx{}
	if err := tx.Deserialize(bytes.NewReader(b)); err != nil {
		return nil, errors.Wrap(err, "deserialize")
	}

	if !txid.Equal(tx.TxHash()) {
		return nil, fmt.Errorf("Wrong txid : got %s, want %s", tx.TxHash(), txid)
	}

	return tx, nil
}

func getWithToken(ctx context.Context, url, token string, response interface{}) error {
	var transport = &http.Transport{
		Dial: (&net.Dialer{
			Timeout: 5 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 5 * time.Second,
	}

	var client = &http.Client{
		Timeout:   time.Second * 10,
		Transport: transport,
	}

	// fmt.Printf("URL : %s\n", url)

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return errors.Wrap(err, "create request")
	}

	if len(token) > 0 {
		httpRequest.Header.Add("woc-api-key", token)
	}

	httpResponse, err := client.Do(httpRequest)
	if err != nil {
		if errors.Cause(err) == context.DeadlineExceeded {
			return errors.Wrap(err, "http post timeout")
		}

		return errors.Wrap(err, "http post")
	}

	if httpResponse.StatusCode < 200 || httpResponse.StatusCode > 299 {
		if httpResponse.Body != nil {
			b, rerr := ioutil.ReadAll(httpResponse.Body)
			if rerr == nil {
				return fmt.Errorf("Status %d: %s", httpResponse.StatusCode, string(b))
			}
		}

		return fmt.Errorf("Status %d", httpResponse.StatusCode)
	}

	defer httpResponse.Body.Close()

	if response != nil {
		if responseString, isString := response.(*string); isString {
			b, err := ioutil.ReadAll(httpResponse.Body)
			if err != nil {
				return errors.Wrap(err, "read body")
			}
			*responseString = string(b)
			return nil
		}

		// b, err := ioutil.ReadAll(httpResponse.Body)
		// if err != nil {
		// 	return errors.Wrap(err, "read body")
		// }
		// fmt.Printf("Raw Response : %s\n", string(b))
		// buf := &bytes.Buffer{}
		// json.Indent(buf, b, "", "  ")
		// fmt.Printf("Response : %s\n", string(buf.Bytes()))
		// if err := json.Unmarshal(b, response); err != nil {
		// 	return errors.Wrap(err, "decode response")
		// }

		if err := json.NewDecoder(httpResponse.Body).Decode(response); err != nil {
			return errors.Wrap(err, "decode response")
		}
	}

	return nil
}
