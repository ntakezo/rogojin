package capture

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// DefaultPowHTTPBaseURL is where the powhttp capture proxy serves its data API
// unless POWHTTP_BASE_URL says otherwise.
const DefaultPowHTTPBaseURL = "http://localhost:7777"

// PowHTTP reads captured entries from a running powhttp proxy's data API.
type PowHTTP struct {
	BaseURL string
	Client  *http.Client
}

// NewPowHTTP builds a reader for the powhttp data API. An empty baseURL falls
// back to POWHTTP_BASE_URL, then to the proxy's default address.
func NewPowHTTP(baseURL string) PowHTTP {
	if baseURL == "" {
		baseURL = os.Getenv("POWHTTP_BASE_URL")
	}
	if baseURL == "" {
		baseURL = DefaultPowHTTPBaseURL
	}
	return PowHTTP{
		BaseURL: strings.TrimSuffix(baseURL, "/"),
		Client:  &http.Client{Timeout: 30 * time.Second},
	}
}

// powEntry mirrors the data API's entry object. Headers arrive as ordered
// [name, value] pairs with the HTTP/2 pseudo-headers inline, which is the whole
// reason this is worth reading over reconstructing a request by hand.
type powEntry struct {
	ID              string `json:"id"`
	URL             string `json:"url"`
	HTTPVersion     string `json:"httpVersion"`
	TransactionType string `json:"transactionType"`
	Request         struct {
		Method  string      `json:"method"`
		Path    string      `json:"path"`
		Headers [][2]string `json:"headers"`
		Body    string      `json:"body"`
	} `json:"request"`
	Response *struct {
		StatusCode int         `json:"statusCode"`
		Headers    [][2]string `json:"headers"`
		Body       string      `json:"body"`
	} `json:"response"`
}

// response reduces the captured reply to what typing it needs. A body that will
// not decode is dropped rather than failing the entry: the request is still
// worth generating without a typed response.
func (e powEntry) response() *Response {
	if e.Response == nil {
		return nil
	}
	out := &Response{StatusCode: e.Response.StatusCode}
	for _, pair := range e.Response.Headers {
		if strings.EqualFold(pair[0], "content-type") {
			if parsed, _, err := mime.ParseMediaType(pair[1]); err == nil {
				out.MediaType = strings.ToLower(parsed)
			}
			break
		}
	}
	if e.Response.Body != "" {
		if decoded, err := base64.StdEncoding.DecodeString(e.Response.Body); err == nil {
			out.Body = decoded
		}
	}
	return out
}

// Entry fetches one captured request. session accepts a session ULID or
// "active" for the session powhttp is currently recording into.
func (p PowHTTP) Entry(ctx context.Context, session, entry string) (Entry, error) {
	if session == "" {
		session = "active"
	}
	if entry == "" {
		return Entry{}, fmt.Errorf("powhttp: entry id is required")
	}

	endpoint := fmt.Sprintf("%s/sessions/%s/entries/%s", p.BaseURL, url.PathEscape(session), url.PathEscape(entry))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Entry{}, err
	}
	res, err := p.Client.Do(req)
	if err != nil {
		return Entry{}, fmt.Errorf("powhttp: reach the capture proxy at %s: %w — is it running?", p.BaseURL, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return Entry{}, fmt.Errorf("powhttp: entry %s in session %s: %s", entry, session, res.Status)
	}

	var raw powEntry
	if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
		return Entry{}, fmt.Errorf("powhttp: decode entry %s: %w", entry, err)
	}
	return raw.normalize()
}

// normalize reduces a data API entry to the capture the renderer consumes,
// rejecting anything that is not a request a client could send again.
func (e powEntry) normalize() (Entry, error) {
	if e.TransactionType != "" && e.TransactionType != "request" {
		return Entry{}, fmt.Errorf("powhttp: entry %s is a %s, not a request — it cannot be replayed", e.ID, e.TransactionType)
	}
	if e.Request.Method == "" {
		return Entry{}, fmt.Errorf("powhttp: entry %s has no request method", e.ID)
	}

	fields := make([]Header, 0, len(e.Request.Headers))
	for _, pair := range e.Request.Headers {
		fields = append(fields, Header{Name: pair[0], Value: pair[1]})
	}
	headers, pseudo := splitHeaders(fields)

	// The body is base64 whether or not it is text; an empty string is simply
	// a request without one.
	var body []byte
	if e.Request.Body != "" {
		decoded, err := base64.StdEncoding.DecodeString(e.Request.Body)
		if err != nil {
			return Entry{}, fmt.Errorf("powhttp: decode body of entry %s: %w", e.ID, err)
		}
		body = decoded
	}

	target := e.URL
	if target == "" {
		target = absoluteURL(fields, e.Request.Path)
	}
	if target == "" {
		return Entry{}, fmt.Errorf("powhttp: entry %s has no URL to request", e.ID)
	}

	return Entry{
		ID:       e.ID,
		Source:   "powhttp entry " + e.ID,
		URL:      target,
		Method:   e.Request.Method,
		Proto:    e.HTTPVersion,
		Headers:  headers,
		Pseudo:   pseudo,
		Body:     body,
		Response: e.response(),
	}, nil
}

// absoluteURL rebuilds the request target from the HTTP/2 pseudo-headers, for
// the entries that carry no absolute URL of their own.
func absoluteURL(fields []Header, path string) string {
	var scheme, authority string
	for _, f := range fields {
		switch f.Name {
		case ":scheme":
			scheme = f.Value
		case ":authority":
			authority = f.Value
		case ":path":
			if path == "" {
				path = f.Value
			}
		}
	}
	if scheme == "" || authority == "" {
		return ""
	}
	return scheme + "://" + authority + path
}
