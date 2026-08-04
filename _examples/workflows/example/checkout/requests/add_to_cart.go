package requests

import (
	"context"
	"strconv"
	"strings"

	http "github.com/bogdanfinn/fhttp"
	"github.com/justhyped/OrderedForm"
)

// AddToCartRequest adds a variant to the cart. The CSRF token rides in a header,
// so it sits beside Body rather than in it.
type AddToCartRequest struct {
	URL       string
	CSRFToken string
	Body      AddToCartBody
}

// AddToCartBody is the form the cart endpoint expects. A form carries no tags:
// the field order here is the order AddToCart encodes them in.
type AddToCartBody struct {
	VariantID string
	Quantity  int
}

// AddToCartResponse carries the created cart's id.
type AddToCartResponse struct {
	CartID string `json:"cartID"`
}

// AddToCart posts the variant as a form body with the CSRF token in a header.
func AddToCart(ctx context.Context, client *http.Client, r AddToCartRequest) (*http.Response, error) {
	// OrderedForm keeps the fields in the order they are set; url.Values would
	// sort the keys instead.
	form := new(OrderedForm.OrderedForm)
	form.Set("variantID", r.Body.VariantID)
	form.Set("quantity", strconv.Itoa(r.Body.Quantity))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, strings.NewReader(form.URLEncode()))
	if err != nil {
		return nil, err
	}

	// content-length and cookie are listed with no Add: the transport and the
	// cookie jar own them, and this is where their values have to land.
	headers := http.Header{
		http.HeaderOrderKey:  {"content-length", "content-type", "x-csrf-token", "cookie"},
		http.PHeaderOrderKey: {":method", ":authority", ":scheme", ":path"},
	}
	headers.Add("content-type", "application/x-www-form-urlencoded")
	headers.Add("x-csrf-token", r.CSRFToken)
	req.Header = headers
	return client.Do(req)
}
