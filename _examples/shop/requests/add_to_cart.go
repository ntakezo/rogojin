package requests

import (
	"context"
	"strings"

	http "github.com/bogdanfinn/fhttp"
	"github.com/justhyped/OrderedForm"
)

// AddToCartBody is the form AddToCart submits. Fields are declared and set in
// the captured order, and are strings because a form carries no other type.
type AddToCartBody struct {
	VariantID string
	Quantity  string
}

// AddToCartRequest is the input to AddToCart. Lift each value that varies per
// task into a field here and reference it below.
type AddToCartRequest struct {
	// Body is the payload, and the only thing encoded. Every request reserves
	// the name, so a field added beside it cannot reach the wire.
	Body AddToCartBody
}

// AddToCartResponse is the body AddToCart returned, typed from the capture. It
// describes that one reply: a field the endpoint only sometimes sends is absent.
type AddToCartResponse struct {
	CartID string `json:"cartID"`
}

// AddToCart sends the request as captured, so every value below is still the
// captured one. Generated from HAR entry add-to-cart of checkout.har.
func AddToCart(ctx context.Context, client *http.Client, r AddToCartRequest) (*http.Response, error) {
	// OrderedForm sends the fields in the order they are set; url.Values sorts.
	form := new(OrderedForm.OrderedForm)
	form.Set("variantID", r.Body.VariantID)
	form.Set("quantity", r.Body.Quantity)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://shop.example.com/cart", strings.NewReader(form.URLEncode()))
	if err != nil {
		return nil, err
	}

	// The order key is the captured send order, matched lowercased.
	// content-length and cookie get no Add: the client fills them in at that position.
	headers := http.Header{
		http.HeaderOrderKey:  {"content-length", "content-type", "user-agent", "origin", "cookie"},
		http.PHeaderOrderKey: {":method", ":authority", ":scheme", ":path"},
	}
	headers.Add("content-type", "application/x-www-form-urlencoded")
	headers.Add("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36")
	headers.Add("origin", "https://shop.example.com")
	req.Header = headers
	return client.Do(req)
}
