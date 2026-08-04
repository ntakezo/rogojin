// package requests holds the site's request functions. Each is pure: given a
// client and a typed request struct it fires one HTTP call and returns the raw
// response, leaving decode and body-close to the caller. The exported response
// struct is the shape that raw body unmarshals into.
package requests

import (
	"context"
	"net/url"

	http "github.com/bogdanfinn/fhttp"
)

// GetHomepageRequest is the product page fetch: the page URL and the size the
// caller is shopping for, carried as a query param.
type GetHomepageRequest struct {
	URL  string
	Size string
}

// GetHomepageResponse is the product page payload decoded for the matching
// variant and the CSRF token later states need.
type GetHomepageResponse struct {
	VariantID string `json:"variantID"`
	CSRFToken string `json:"csrfToken"`
}

// GetHomepage fetches the product page for the requested size.
func GetHomepage(ctx context.Context, client *http.Client, r GetHomepageRequest) (*http.Response, error) {
	endpoint := r.URL + "?size=" + url.QueryEscape(r.Size)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	// The order keys are the order fhttp sends in, which it matches lowercased.
	// cookie is listed with no Add: the jar owns it, and this is where its value
	// has to land.
	headers := http.Header{
		http.HeaderOrderKey:  {"accept", "accept-language", "cookie"},
		http.PHeaderOrderKey: {":method", ":authority", ":scheme", ":path"},
	}
	headers.Add("accept", "application/json")
	headers.Add("accept-language", "en-US,en;q=0.9")
	req.Header = headers
	return client.Do(req)
}
