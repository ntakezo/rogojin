package requests

import (
	"bytes"
	"context"
	"encoding/json"

	http "github.com/bogdanfinn/fhttp"
)

// LoginRequest starts the site's mail-verified sign-in. URL is excluded from
// the body; the remaining fields marshal into the JSON payload.
type LoginRequest struct {
	URL       string `json:"-"`
	Email     string `json:"email"`
	CSRFToken string `json:"csrfToken"`
}

// LoginResponse acknowledges that the verification mail is on its way.
type LoginResponse struct {
	Status string `json:"status"`
}

// Login posts the persona's email to begin sign-in; the site answers by mail.
func Login(ctx context.Context, client *http.Client, r LoginRequest) (*http.Response, error) {
	body, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return client.Do(req)
}
