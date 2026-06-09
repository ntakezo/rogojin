package requests

import (
	"bytes"
	"context"
	"encoding/json"

	http "github.com/bogdanfinn/fhttp"
)

// SubmitCheckoutRequest finalizes the order. URL is excluded from the body; the
// remaining fields marshal directly into the JSON payload the endpoint expects.
type SubmitCheckoutRequest struct {
	URL     string `json:"-"`
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
	body, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return client.Do(req)
}
