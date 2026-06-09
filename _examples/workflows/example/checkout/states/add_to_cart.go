package states

import (
	"context"
	"encoding/json"

	"github.com/ntakezo/rogojin/_examples/workflows/example/checkout/requests"
	"github.com/ntakezo/rogojin/workflows"
)

const addToCart workflows.State = "add-to-cart"

// AddToCart posts the captured variant and CSRF token to the cart endpoint and
// records the resulting cart id for the checkout state.
func (c *Context) AddToCart(ctx context.Context) (*workflows.State, error) {
	base, err := origin(c.static.ProductURL)
	if err != nil {
		return nil, err
	}
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}

	res, err := requests.AddToCart(ctx, client, requests.AddToCartRequest{
		URL:       base + "/cart",
		VariantID: c.running.variantID,
		CSRFToken: c.running.csrfToken,
		Quantity:  1,
	})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var body requests.AddToCartResponse
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, err
	}
	c.running.cartID = body.CartID

	return workflows.Next(submitCheckout), nil
}
