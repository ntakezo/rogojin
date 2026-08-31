package example_checkout

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
	base, err := origin(c.in.ProductURL)
	if err != nil {
		return nil, err
	}
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}

	res, err := requests.AddToCart(ctx, client, requests.AddToCartRequest{
		URL:       base + "/cart",
		VariantID: c.d.Homepage.VariantID,
		CSRFToken: c.d.Homepage.CSRFToken,
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
	c.d.Cart.ID = body.CartID

	return workflows.Next(submitCheckout), nil
}
