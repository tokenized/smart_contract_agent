package whatsonchain

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
)

const (
	MaxTxRequestCount = 20 // max txs to request from URLGetRawTxs

	URLGetAddressHistory = "https://api.whatsonchain.com/v1/bsv/%s/address/%s/history"
	URLGetRawTx          = "https://api.whatsonchain.com/v1/bsv/%s/tx/%s/hex"
	URLGetRawTxs         = "https://api.whatsonchain.com/v1/bsv/%s/txs/hex"
)

var (
	ErrTimeout = errors.New("Timed Out")
)

type Service struct {
	apiKey  string
	network bitcoin.Network
}

type HTTPError struct {
	Status  int
	Message string
}

type bulkTxRequest struct {
	TxIDs []*bitcoin.Hash32 `json:"txids"`
}

type HistoryItem struct {
	TxID   bitcoin.Hash32 `json:"tx_hash"`
	Height int            `json:"height"`
}

type History []*HistoryItem

func (err HTTPError) Error() string {
	if len(err.Message) > 0 {
		return fmt.Sprintf("HTTP Status %d : %s", err.Status, err.Message)
	}

	return fmt.Sprintf("HTTP Status %d", err.Status)
}

func NewService(apiKey string, net bitcoin.Network) *Service {
	return &Service{
		apiKey:  apiKey,
		network: net,
	}
}

func (s *Service) NetworkName() string {
	return NetworkName(s.network)
}

func NetworkName(net bitcoin.Network) string {
	switch net {
	case bitcoin.MainNet:
		return "main"
	default:
		return "test"
	}
}

func (s *Service) GetLockingScriptHistory(ctx context.Context,
	lockingScript bitcoin.Script) (History, error) {
	ra, err := bitcoin.RawAddressFromLockingScript(lockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "address")
	}

	url := fmt.Sprintf(URLGetAddressHistory, s.NetworkName(),
		bitcoin.NewAddressFromRawAddress(ra, s.network))

	var response History
	if err := getWithToken(ctx, url, s.apiKey, &response); err != nil {
		return nil, errors.Wrap(err, "get")
	}

	return response, nil
}

func (s *Service) GetTx(ctx context.Context, txid bitcoin.Hash32) (*wire.MsgTx, error) {
	url := fmt.Sprintf(URLGetRawTx, s.NetworkName(), txid)

	var response string
	if err := getWithToken(ctx, url, s.apiKey, &response); err != nil {
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

// postWithToken sends a request to the HTTP server using the POST method with an authentication
// header token.
func postWithToken(ctx context.Context, url, token string, request, response interface{}) error {
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

	var r io.Reader
	if request != nil {
		var b []byte
		if s, ok := request.(string); ok {
			// request is already a json string, not an object to convert to json
			b = []byte(s)
		} else {
			bt, err := json.Marshal(request)
			if err != nil {
				return errors.Wrap(err, "marshal request")
			}
			b = bt
		}
		r = bytes.NewReader(b)
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, url, r)
	if err != nil {
		return errors.Wrap(err, "create request")
	}

	if len(token) > 0 {
		httpRequest.Header.Add("woc-api-key", token)
	}

	if request != nil {
		httpRequest.Header.Add("Content-Type", "application/json")
	}

	httpResponse, err := client.Do(httpRequest)
	if err != nil {
		if errors.Cause(err) == context.DeadlineExceeded {
			return errors.Wrap(ErrTimeout, errors.Wrap(err, "http post").Error())
		}

		return errors.Wrap(err, "http post")
	}

	if httpResponse.StatusCode < 200 || httpResponse.StatusCode > 299 {
		if httpResponse.Body != nil {
			b, rerr := ioutil.ReadAll(httpResponse.Body)
			if rerr == nil {
				return HTTPError{
					Status:  httpResponse.StatusCode,
					Message: string(b),
				}
			}
		}

		return HTTPError{Status: httpResponse.StatusCode}
	}

	defer httpResponse.Body.Close()

	if response != nil {
		if responseString, isString := response.(*string); isString {
			b, err := ioutil.ReadAll(httpResponse.Body)
			if err != nil {
				return errors.Wrap(err, "read body")
			}
			*responseString = string(b)
		}

		if err := json.NewDecoder(httpResponse.Body).Decode(response); err != nil {
			return errors.Wrap(err, "decode response")
		}
	}

	return nil
}

// getWithToken sends a request to the HTTP server using the GET method with an authentication
// header token.
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
			return errors.Wrap(ErrTimeout, errors.Wrap(err, "http post").Error())
		}

		return errors.Wrap(err, "http post")
	}

	if httpResponse.StatusCode < 200 || httpResponse.StatusCode > 299 {
		if httpResponse.Body != nil {
			b, rerr := ioutil.ReadAll(httpResponse.Body)
			if rerr == nil {
				return HTTPError{
					Status:  httpResponse.StatusCode,
					Message: string(b),
				}
			}
		}

		return HTTPError{Status: httpResponse.StatusCode}
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
