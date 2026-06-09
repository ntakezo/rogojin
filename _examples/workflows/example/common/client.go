// package common holds side-effect ports shared by the example workflow's states,
// starting with the fhttp client every task talks to the site through.
package common

import (
	"fmt"
	"net/url"

	http "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/fhttp/cookiejar"
)

// NewClient builds an fhttp client backed by an isolated, per-task cookie jar so
// each task carries its own session, routing through proxyURL when one is given.
func NewClient(proxyURL string) (*http.Client, error) {
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	if proxyURL == "" {
		return client, nil
	}

	u, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("parse proxy url: %w", err)
	}
	client.Transport = &http.Transport{Proxy: http.ProxyURL(u)}
	return client, nil
}
