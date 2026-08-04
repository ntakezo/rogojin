package states

import (
	"context"
	"encoding/json"

	"github.com/ntakezo/rogojin/_examples/shop/requests"
	"github.com/ntakezo/rogojin/workflows"
)

const submitCheckout workflows.State = "submit-checkout"

// SubmitCheckout runs the submit-checkout state. Fill the request from the context, and
// read what came back onto c.r for the states after it.
func (c *Context) SubmitCheckout(ctx context.Context) (*workflows.State, error) {
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}
	res, err := requests.SubmitCheckout(ctx, client, requests.SubmitCheckoutRequest{})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var body requests.SubmitCheckoutResponse
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, err
	}

	return nil, nil
}
