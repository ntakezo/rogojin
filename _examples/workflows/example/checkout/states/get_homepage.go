package states

import (
	"context"
	"encoding/json"

	"github.com/ntakezo/rogojin/_examples/workflows/example/checkout/requests"
	"github.com/ntakezo/rogojin/workflows"
)

const getHomepage workflows.State = "get-homepage"

// GetHomepage fetches the product page and records the variant matching the
// requested size and the CSRF token, both written to the running context.
func (c *Context) GetHomepage(ctx context.Context) (*workflows.State, error) {
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}

	res, err := requests.GetHomepage(ctx, client, requests.GetHomepageRequest{
		URL:  c.static.ProductURL,
		Size: c.static.Size,
	})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var body requests.GetHomepageResponse
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, err
	}
	c.running.variantID = body.VariantID
	c.running.csrfToken = body.CSRFToken

	return workflows.Next(waitInQueue), nil
}
