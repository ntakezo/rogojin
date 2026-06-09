package requests

import (
	"context"
	"strconv"
	"strings"

	http "github.com/bogdanfinn/fhttp"
	"github.com/justhyped/OrderedForm"
)

// AddToCartRequest adds a variant to the cart. The CSRF token rides in a header;
// the variant and quantity are form-encoded, as a typical cart POST expects.
type AddToCartRequest struct {
	URL       string
	VariantID string
	CSRFToken string
	Quantity  int
}

// AddToCartResponse carries the created cart's id.
type AddToCartResponse struct {
	CartID string `json:"cartID"`
}

// AddToCart posts the variant as a form body with the CSRF token in a header.
func AddToCart(ctx context.Context, client *http.Client, r AddToCartRequest) (*http.Response, error) {
	form := new(OrderedForm.OrderedForm)
	form.Set("variantID", r.VariantID)
	form.Set("quantity", strconv.Itoa(r.Quantity))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, strings.NewReader(form.URLEncode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-CSRF-Token", r.CSRFToken)
	req.Header[http.HeaderOrderKey] = []string{"Content-Type", "X-CSRF-Token"}
	return client.Do(req)
}
