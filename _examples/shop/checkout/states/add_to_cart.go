package states

import (
	"context"
	"encoding/json"

	"github.com/ntakezo/rogojin/_examples/shop/requests"
	"github.com/ntakezo/rogojin/workflows"
)

const addToCart workflows.State = "add-to-cart"

// AddToCart runs the add-to-cart state. Fill the request from the context, and
// read what came back onto c.r for the states after it.
func (c *Context) AddToCart(ctx context.Context) (*workflows.State, error) {
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}
	res, err := requests.AddToCart(ctx, client, requests.AddToCartRequest{})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var body requests.AddToCartResponse
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, err
	}

	return workflows.Next(submitCheckout), nil
}
