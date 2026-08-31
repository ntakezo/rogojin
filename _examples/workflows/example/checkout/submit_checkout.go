package example_checkout

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
//
// Placing an order is the one side effect a re-run must never repeat, so the
// state is guarded: the placed order is checkpointed the moment it confirms,
// and a re-entered state — recovery, or a retry after a later error — sees it
// in the snapshot and skips the submit.
func (c *Context) SubmitCheckout(ctx context.Context) (*workflows.State, error) {
	if c.running.order.OrderID != "" {
		return nil, nil // the order already went through; never place it twice
	}

	base, err := origin(c.static.ProductURL)
	if err != nil {
		return nil, err
	}
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}
	profile, err := c.profile(ctx)
	if err != nil {
		return nil, err
	}

	res, err := requests.SubmitCheckout(ctx, client, requests.SubmitCheckoutRequest{
		URL:     base + "/checkout",
		CartID:  c.running.cartID,
		Email:   profile.Email,
		Name:    profile.Name,
		Address: profile.Address,
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

	// The order exists in the world now: make that durable before returning,
	// so a crash between here and the terminal stamp cannot resubmit it.
	if err := c.running.checkpoint(ctx); err != nil {
		return nil, err
	}

	return nil, nil
}
