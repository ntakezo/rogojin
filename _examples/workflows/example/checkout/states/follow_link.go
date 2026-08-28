package states

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ntakezo/rogojin/_examples/workflows/example/checkout/requests"
	"github.com/ntakezo/rogojin/workflows"
)

const followLink workflows.State = "follow-link"

// FollowLink visits the link parsed from the verification mail, completing
// the sign-in. The link is durable state: a task recovered here resumes
// from the snapshot without re-reading the mail.
func (c *Context) FollowLink(ctx context.Context) (*workflows.State, error) {
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}

	res, err := requests.FollowLink(ctx, client, requests.FollowLinkRequest{URL: c.running.verifyURL})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	var body requests.FollowLinkResponse
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, err
	}
	if body.Status != "verified" {
		return nil, fmt.Errorf("verification link answered %q, want verified", body.Status)
	}

	return workflows.Next(addToCart), nil
}
