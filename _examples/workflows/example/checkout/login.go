package example_checkout

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ntakezo/rogojin/_examples/workflows/example/checkout/requests"
	"github.com/ntakezo/rogojin/workflows"
)

const login workflows.State = "login"

// Login signs the persona in and waits on its forwarding inbox for the
// site's verification mail, parsing the link to follow out of the body. The
// backfill window opens just before the login request, so mail that lands
// while the subscription is still being established — or while a recovered
// task was down — is re-read from the server rather than lost.
func (c *Context) Login(ctx context.Context) (*workflows.State, error) {
	client, err := c.client(ctx)
	if err != nil {
		return nil, err
	}
	profile, err := c.profile(ctx)
	if err != nil {
		return nil, err
	}
	site, err := origin(c.static.ProductURL)
	if err != nil {
		return nil, err
	}

	started := time.Now().Add(-time.Second)
	res, err := requests.Login(ctx, client, requests.LoginRequest{
		URL:       site + "/login",
		Email:     profile.Email,
		CSRFToken: c.running.csrfToken,
	})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	var body requests.LoginResponse
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, err
	}

	sub, err := c.inbox(ctx, started)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-sub.C():
		if !ok {
			return nil, fmt.Errorf("inbox closed before the verification mail arrived")
		}
		link := firstURL(msg.Text)
		if link == "" {
			return nil, fmt.Errorf("verification mail %q carried no link", msg.Subject)
		}
		c.running.verifyURL = link
		fmt.Printf("  task %s got %q from %s, link %s\n", c.running.account.TaskID, msg.Subject, msg.From, link)
	}
	return workflows.Next(followLink), nil
}

// firstURL fake-parses a mail body: the first http(s) token is the link. A
// real workflow would parse the HTML part properly.
func firstURL(text string) string {
	for _, field := range strings.Fields(text) {
		if strings.HasPrefix(field, "http://") || strings.HasPrefix(field, "https://") {
			return field
		}
	}
	return ""
}
