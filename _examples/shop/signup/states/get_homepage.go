package states

import (
	"context"

	"github.com/ntakezo/rogojin/_examples/shop/requests"
	"github.com/ntakezo/rogojin/workflows"
)

const getHomepage workflows.State = "get-homepage"

// GetHomepage runs the get-homepage state. Fill the request from the context, and
// read what came back onto c.r for the states after it.
func (c *Context) GetHomepage(ctx context.Context) (*workflows.State, error) {
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}
	res, err := requests.GetHomepage(ctx, client, requests.GetHomepageRequest{})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	// Nothing typed from this response, so reading it is yours: res.Body holds
	// the reply exactly as it came back.

	return workflows.Next(submitSignup), nil
}
