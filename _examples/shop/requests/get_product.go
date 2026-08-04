package requests

import (
	"context"

	http "github.com/bogdanfinn/fhttp"
)

// GetProductRequest is the input to GetProduct. Lift each value that varies per
// task into a field here and reference it below.
type GetProductRequest struct {
	// Body is the payload, and the only thing encoded. Every request reserves
	// the name; this request sends no body, so it stays nil.
	Body any
}

// GetProductResponse is the body GetProduct returned, typed from the capture. It
// describes that one reply: a field the endpoint only sometimes sends is absent.
type GetProductResponse struct {
	InStock   bool   `json:"inStock"`
	Price     int64  `json:"price"`
	VariantID string `json:"variantID"`
}

// GetProduct sends the request as captured, so every value below is still the
// captured one. Generated from HAR entry get-product of checkout.har.
func GetProduct(ctx context.Context, client *http.Client, r GetProductRequest) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://shop.example.com/api/product/example-tee?size=M", nil)
	if err != nil {
		return nil, err
	}

	// The order key is the captured send order, matched lowercased.
	// cookie get no Add: the client fills them in at that position.
	headers := http.Header{
		http.HeaderOrderKey:  {"accept", "user-agent", "referer", "accept-language", "accept-encoding", "cookie"},
		http.PHeaderOrderKey: {":method", ":authority", ":scheme", ":path"},
	}
	headers.Add("accept", "application/json")
	headers.Add("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36")
	headers.Add("referer", "https://shop.example.com/")
	headers.Add("accept-language", "en-US,en;q=0.9")
	headers.Add("accept-encoding", "gzip, deflate, br")
	req.Header = headers
	return client.Do(req)
}
