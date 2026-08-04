package states

import (
	"context"
	"encoding/json"

	"github.com/ntakezo/rogojin/_examples/shop/requests"
	"github.com/ntakezo/rogojin/workflows"
)

const submitSignup workflows.State = "submit-signup"

// SubmitSignup runs the submit-signup state. Fill the request from the context, and
// read what came back onto c.r for the states after it.
func (c *Context) SubmitSignup(ctx context.Context) (*workflows.State, error) {
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}
	res, err := requests.SubmitSignup(ctx, client, requests.SubmitSignupRequest{})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var body requests.SubmitSignupResponse
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, err
	}

	return nil, nil
}
