package common

import (
	"errors"
	"fmt"
	"net/url"

	http "github.com/bogdanfinn/fhttp"
)

// SetCookies installs cookies into client's jar under the URL's host, so later
// requests through client carry them. It works through the jar interface — no
// type assertion back to the concrete jar — and fails loud on a client with no
// jar or a URL with no scheme or host, the two silent ways a cookie can
// otherwise vanish.
func SetCookies(client *http.Client, rawURL string, cookies ...*http.Cookie) error {
	if client == nil || client.Jar == nil {
		return errors.New("client has no cookie jar")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse cookie url: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("cookie url %q has no scheme or host", rawURL)
	}
	client.Jar.SetCookies(u, cookies)
	return nil
}
