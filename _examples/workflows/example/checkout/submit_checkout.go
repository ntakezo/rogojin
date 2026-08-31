package example_checkout

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ntakezo/rogojin/_examples/workflows/example/checkout/requests"
	"github.com/ntakezo/rogojin/workflows"
)

const submitCheckout workflows.State = "submit-checkout"

// SubmitCheckout posts the profile and cart id to place the order. It is the
// terminal state, so it returns a nil next state.
//
// Placing an order is the one side effect a re-run must never repeat, so the
// submit rides in workflows.Do: the moment the order confirms, the result is
// recorded in the effect log and checkpointed, and a re-entered state —
// recovery, or the graph's Retry — replays the recorded order instead of
// resubmitting. A submit that failed records nothing, so retrying it stays
// safe; an unconfirmed order is marked Permanent, since resubmitting a
// rejection will not change the answer.
func (c *Context) SubmitCheckout(ctx context.Context) (*workflows.State, error) {
	order, err := workflows.Do(ctx, &c.Base, "submit-order",
		func(ctx context.Context) (requests.SubmitCheckoutResponse, error) {
			var zero requests.SubmitCheckoutResponse
			base, err := origin(c.in.ProductURL)
			if err != nil {
				return zero, err
			}
			client, err := c.client(ctx)
			if err != nil {
				return zero, err
			}
			profile, err := c.profile(ctx)
			if err != nil {
				return zero, err
			}

			res, err := requests.SubmitCheckout(ctx, client, requests.SubmitCheckoutRequest{
				URL:     base + "/checkout",
				CartID:  c.d.Cart.ID,
				Email:   profile.Email,
				Name:    profile.Name,
				Address: profile.Address,
			})
			if err != nil {
				return zero, err
			}
			defer res.Body.Close()

			var body requests.SubmitCheckoutResponse
			if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
				return zero, err
			}
			if body.Status != "confirmed" {
				return zero, workflows.Permanent(
					fmt.Errorf("checkout: order %q not confirmed: status %q", body.OrderID, body.Status))
			}
			return body, nil
		})
	if err != nil {
		return nil, err
	}
	c.order = order

	return nil, nil
}
