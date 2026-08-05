package states

import (
	"context"
	"encoding/json"

	"github.com/ntakezo/rogojin/_examples/workflows/shop/requests"
	"github.com/ntakezo/rogojin/workflows"
)

const getProduct workflows.State = "get-product"

// GetProduct runs the get-product state. Fill the request from the context, and
// read what came back onto c.r for the states after it.
func (c *Context) GetProduct(ctx context.Context) (*workflows.State, error) {
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}
	res, err := requests.GetProduct(ctx, client, requests.GetProductRequest{})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var body requests.GetProductResponse
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, err
	}

	return workflows.Next(addToCart), nil
}
