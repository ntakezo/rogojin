package requests

import (
	"context"

	http "github.com/bogdanfinn/fhttp"
)

// FollowLinkRequest visits the verification link exactly as the mail gave it.
type FollowLinkRequest struct {
	URL string
}

// FollowLinkResponse reports whether the link completed the sign-in.
type FollowLinkResponse struct {
	Status string `json:"status"`
}

// FollowLink fetches the verification link from the sign-in mail.
func FollowLink(ctx context.Context, client *http.Client, r FollowLinkRequest) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header[http.HeaderOrderKey] = []string{"Accept"}
	return client.Do(req)
}
