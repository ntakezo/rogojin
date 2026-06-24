package states

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ntakezo/rogojin/_examples/workflows/example/checkout/requests"
	"github.com/ntakezo/rogojin/workflows"
)

const submitCheckout workflows.State = "submit-checkout"

// SubmitCheckout posts the profile and cart id to place the order, failing if it
// comes back unconfirmed. It is the terminal state, so it returns a nil next state.
func (c *Context) SubmitCheckout(ctx context.Context) (*workflows.State, error) {
	base, err := origin(c.static.ProductURL)
	if err != nil {
		return nil, err
	}
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}

	res, err := requests.SubmitCheckout(ctx, client, requests.SubmitCheckoutRequest{
		URL:     base + "/checkout",
		CartID:  c.running.cartID,
		Email:   c.static.Profile.Email,
		Name:    c.static.Profile.Name,
		Address: c.static.Profile.Address,
	})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var body requests.SubmitCheckoutResponse
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, err
	}
	if body.Status != "confirmed" {
		return nil, fmt.Errorf("checkout: order %q not confirmed: status %q", body.OrderID, body.Status)
	}
	c.running.order = body

	return nil, nil
}
