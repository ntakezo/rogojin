package requests

import (
	"bytes"
	"context"
	"encoding/json"

	http "github.com/bogdanfinn/fhttp"
)

// SubmitCheckoutBody is the JSON body SubmitCheckout sends. Fields are declared in the
// captured order, which is the order they are marshaled in.
type SubmitCheckoutBody struct {
	CartID  string `json:"cartID"`
	Email   string `json:"email"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

// SubmitCheckoutRequest is the input to SubmitCheckout. Lift each value that varies per
// task into a field here and reference it below.
type SubmitCheckoutRequest struct {
	// Body is the payload, and the only thing encoded. Every request reserves
	// the name, so a field added beside it cannot reach the wire.
	Body SubmitCheckoutBody
}

// SubmitCheckoutResponse is the body SubmitCheckout returned, typed from the capture. It
// describes that one reply: a field the endpoint only sometimes sends is absent.
type SubmitCheckoutResponse struct {
	OrderID string `json:"orderID"`
	Status  string `json:"status"`
}

// SubmitCheckout sends the request as captured, so every value below is still the
// captured one. Generated from HAR entry submit-checkout of checkout.har.
func SubmitCheckout(ctx context.Context, client *http.Client, r SubmitCheckoutRequest) (*http.Response, error) {
	// json.Marshal escapes <, > and & to \u00xx; the captured body did not.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(r.Body); err != nil {
		return nil, err
	}
	body := bytes.TrimRight(buf.Bytes(), "\n")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://shop.example.com/checkout", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	// The order key is the captured send order, matched lowercased.
	// content-length and cookie get no Add: the client fills them in at that position.
	headers := http.Header{
		http.HeaderOrderKey:  {"content-length", "content-type", "user-agent", "origin", "cookie"},
		http.PHeaderOrderKey: {":method", ":authority", ":scheme", ":path"},
	}
	headers.Add("content-type", "application/json")
	headers.Add("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36")
	headers.Add("origin", "https://shop.example.com")
	req.Header = headers
	return client.Do(req)
}
