package example_checkout

import (
	"context"
	"encoding/json"

	"github.com/ntakezo/rogojin/_examples/workflows/example/checkout/requests"
	"github.com/ntakezo/rogojin/workflows"
)

const getHomepage workflows.State = "get-homepage"

// GetHomepage fetches the product page and records the variant matching the
// requested size and the CSRF token in its durable section.
func (c *Context) GetHomepage(ctx context.Context) (*workflows.State, error) {
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}

	res, err := requests.GetHomepage(ctx, client, requests.GetHomepageRequest{
		URL:  c.in.ProductURL,
		Size: c.in.Size,
	})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var body requests.GetHomepageResponse
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, err
	}
	c.d.Homepage = HomepageState{VariantID: body.VariantID, CSRFToken: body.CSRFToken}

	return workflows.Next(waitInQueue), nil
}
