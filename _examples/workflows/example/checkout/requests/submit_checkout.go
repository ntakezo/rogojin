package requests

import (
	"bytes"
	"context"
	"encoding/json"

	http "github.com/bogdanfinn/fhttp"
)

// SubmitCheckoutRequest finalizes the order. Only Body is marshaled, so URL —
// and anything else added here — cannot reach the payload.
type SubmitCheckoutRequest struct {
	URL  string
	Body SubmitCheckoutBody
}

// SubmitCheckoutBody is the JSON payload the checkout endpoint expects.
type SubmitCheckoutBody struct {
	CartID  string `json:"cartID"`
	Email   string `json:"email"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

// SubmitCheckoutResponse carries the placed order's id and its status.
type SubmitCheckoutResponse struct {
	OrderID string `json:"orderID"`
	Status  string `json:"status"`
}

// SubmitCheckout posts the cart id and buyer profile as a JSON body to place the order.
func SubmitCheckout(ctx context.Context, client *http.Client, r SubmitCheckoutRequest) (*http.Response, error) {
	body, err := json.Marshal(r.Body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Append("Content-Type", "application/json")
	req.Header[http.PHeaderOrderKey] = []string{":method", ":authority", ":scheme", ":path"}
	return client.Do(req)
}
