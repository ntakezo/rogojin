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
// submit rides in workflows.Once: the moment it confirms, the guard and the
// placed order are checkpointed, and a re-entered state — recovery, or a
// retry after a later error — skips the submit. A submit that failed leaves
// the guard down, so retrying it stays safe.
func (c *Context) SubmitCheckout(ctx context.Context) (*workflows.State, error) {
	err := workflows.Once(ctx, &c.running.submitted, c.running.checkpoint, func(ctx context.Context) error {
		base, err := origin(c.static.ProductURL)
		if err != nil {
			return err
		}
		client, err := c.client(ctx)
		if err != nil {
			return err
		}
		profile, err := c.profile(ctx)
		if err != nil {
			return err
		}

		res, err := requests.SubmitCheckout(ctx, client, requests.SubmitCheckoutRequest{
			URL:     base + "/checkout",
			CartID:  c.running.cartID,
			Email:   profile.Email,
			Name:    profile.Name,
			Address: profile.Address,
		})
		if err != nil {
			return err
		}
		defer res.Body.Close()

		var body requests.SubmitCheckoutResponse
		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			return err
		}
		if body.Status != "confirmed" {
			return fmt.Errorf("checkout: order %q not confirmed: status %q", body.OrderID, body.Status)
		}
		c.running.order = body
		return nil
	})
	if err != nil {
		return nil, err
	}

	return nil, nil
}
