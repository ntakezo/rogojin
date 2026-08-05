package requests

import (
	"context"

	http "github.com/bogdanfinn/fhttp"
)

// GetHomepageRequest is the input to GetHomepage. Lift each value that varies per
// task into a field here and reference it below.
type GetHomepageRequest struct {
	// Body is the payload, and the only thing encoded. Every request reserves
	// the name; this request sends no body, so it stays nil.
	Body any
}

// GetHomepageResponse is the shape GetHomepage's body unmarshals into: give it the
// fields the caller reads. Nothing typed from a text/html response.
type GetHomepageResponse struct{}

// GetHomepage sends the request as captured, so every value below is still the
// captured one. Generated from HAR entry get-homepage of checkout.har.
func GetHomepage(ctx context.Context, client *http.Client, r GetHomepageRequest) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://shop.example.com/", nil)
	if err != nil {
		return nil, err
	}

	// The order key is the captured send order, matched lowercased.
	headers := http.Header{
		http.HeaderOrderKey:  {"accept", "user-agent", "accept-language", "accept-encoding"},
		http.PHeaderOrderKey: {":method", ":authority", ":scheme", ":path"},
	}
	headers.Add("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	headers.Add("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36")
	headers.Add("accept-language", "en-US,en;q=0.9")
	headers.Add("accept-encoding", "gzip, deflate, br")
	req.Header = headers
	return client.Do(req)
}
