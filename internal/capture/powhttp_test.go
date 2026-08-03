package capture

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// serve stands up a data API returning body for any entry request, so the
// adapter is exercised over real HTTP without needing a capture proxy.
func serve(t *testing.T, status int, body string) PowHTTP {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/entries/") {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	p := NewPowHTTP(srv.URL)
	return p
}

// TestEntryReadsCapturedOrder is the contract the whole generator rests on: the
// wire order of the headers survives the round trip through the data API, the
// pseudo-headers are split out of the ordinary ones, and Content-Length — which
// the transport owns — is dropped.
func TestEntryReadsCapturedOrder(t *testing.T) {
	fixture, err := os.ReadFile("testdata/entry_get.json")
	if err != nil {
		t.Fatal(err)
	}
	got, err := serve(t, http.StatusOK, string(fixture)).Entry(context.Background(), "active", "01KYWPK6HYV98GA9YTMG9TYAX2")
	if err != nil {
		t.Fatalf("Entry: %v", err)
	}

	if want := "https://www.example.com/"; got.URL != want {
		t.Errorf("URL = %q, want %q", got.URL, want)
	}
	if got.Method != http.MethodGet {
		t.Errorf("Method = %q, want GET", got.Method)
	}
	if len(got.Body) != 0 {
		t.Errorf("Body = %q, want empty", got.Body)
	}

	wantPseudo := []string{":method", ":authority", ":scheme", ":path"}
	if strings.Join(got.Pseudo, ",") != strings.Join(wantPseudo, ",") {
		t.Errorf("Pseudo = %v, want %v", got.Pseudo, wantPseudo)
	}

	wantOrder := []string{
		"sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform", "upgrade-insecure-requests",
		"user-agent", "accept", "sec-fetch-site", "sec-fetch-mode", "sec-fetch-user",
		"sec-fetch-dest", "accept-encoding", "accept-language", "priority",
	}
	var order []string
	for _, h := range got.Headers {
		order = append(order, h.Name)
	}
	if strings.Join(order, ",") != strings.Join(wantOrder, ",") {
		t.Errorf("header order =\n  %v\nwant\n  %v", order, wantOrder)
	}
}

// entryJSON builds a data API payload around the given request headers and body.
func entryJSON(t *testing.T, method string, headers [][2]string, body []byte) string {
	t.Helper()
	payload := map[string]any{
		"id":              "01TEST",
		"url":             "https://example.com/cart",
		"httpVersion":     "HTTP/2",
		"transactionType": "request",
		"request": map[string]any{
			"method":  method,
			"path":    "/cart",
			"headers": headers,
			"body":    base64.StdEncoding.EncodeToString(body),
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestEntryKeepsRepeatedHeaders guards the case that a header map would destroy:
// HTTP/2 splits a long cookie into several fields, and each one has to survive
// in place or the request that goes out is not the request that was captured.
func TestEntryKeepsRepeatedHeaders(t *testing.T) {
	headers := [][2]string{
		{":method", "POST"},
		{":path", "/cart"},
		{"content-length", "9"},
		{"content-type", "application/x-www-form-urlencoded"},
		{"cookie", "a=1"},
		{"cookie", "b=2"},
		{"cookie", "c=3"},
	}
	got, err := serve(t, http.StatusOK, entryJSON(t, "POST", headers, []byte("variant=1"))).
		Entry(context.Background(), "active", "01TEST")
	if err != nil {
		t.Fatalf("Entry: %v", err)
	}

	var cookies int
	for _, h := range got.Headers {
		if h.Name == "content-length" {
			t.Error("Content-Length survived; the transport derives it from the body")
		}
		if h.Name == "cookie" {
			cookies++
		}
	}
	if cookies != 3 {
		t.Errorf("kept %d cookie fields, want 3", cookies)
	}
	if string(got.Body) != "variant=1" {
		t.Errorf("Body = %q, want %q", got.Body, "variant=1")
	}
}

// TestEntryReadsResponse covers the reply side: the media type is stripped of
// its parameters, and the body arrives decoded. powhttp stores bodies already
// decompressed, so a gzip response is plain text here and needs no unwrapping.
func TestEntryReadsResponse(t *testing.T) {
	raw := `{"id":"01TEST","url":"https://e.com/","transactionType":"request",
	  "request":{"method":"GET","headers":[],"body":""},
	  "response":{"statusCode":201,
	    "headers":[["content-encoding","gzip"],["Content-Type","application/json; charset=utf-8"]],
	    "body":"eyJvayI6dHJ1ZX0="}}`
	got, err := serve(t, http.StatusOK, raw).Entry(context.Background(), "active", "01TEST")
	if err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if got.Response == nil {
		t.Fatal("Response is nil")
	}
	if got.Response.StatusCode != 201 {
		t.Errorf("StatusCode = %d, want 201", got.Response.StatusCode)
	}
	if got.Response.MediaType != "application/json" {
		t.Errorf("MediaType = %q, want application/json", got.Response.MediaType)
	}
	if string(got.Response.Body) != `{"ok":true}` {
		t.Errorf("Body = %q, want %q", got.Response.Body, `{"ok":true}`)
	}
}

// TestEntryToleratesUnusableResponse keeps a reply that cannot be read from
// costing us the request, which is the half worth generating either way.
func TestEntryToleratesUnusableResponse(t *testing.T) {
	for name, response := range map[string]string{
		"absent":     `null`,
		"no body":    `{"statusCode":204,"headers":[],"body":""}`,
		"bad base64": `{"statusCode":200,"headers":[["content-type","application/json"]],"body":"!!!"}`,
		"no type":    `{"statusCode":200,"headers":[],"body":"eyJvayI6dHJ1ZX0="}`,
	} {
		t.Run(name, func(t *testing.T) {
			raw := `{"id":"01TEST","url":"https://e.com/","transactionType":"request",
			  "request":{"method":"GET","headers":[],"body":""},"response":` + response + `}`
			got, err := serve(t, http.StatusOK, raw).Entry(context.Background(), "active", "01TEST")
			if err != nil {
				t.Fatalf("an unreadable response failed the whole entry: %v", err)
			}
			if got.URL != "https://e.com/" {
				t.Errorf("request was lost with the response: URL = %q", got.URL)
			}
		})
	}
}

// TestEntryRebuildsURLFromPseudoHeaders covers entries that carry no absolute
// URL of their own, where the HTTP/2 pseudo-headers are the only target.
func TestEntryRebuildsURLFromPseudoHeaders(t *testing.T) {
	raw := `{"id":"01TEST","url":"","httpVersion":"HTTP/2","transactionType":"request",
	  "request":{"method":"GET","path":"","headers":[[":scheme","https"],[":authority","api.example.com"],[":path","/v1/cart?a=1"]],"body":""}}`
	got, err := serve(t, http.StatusOK, raw).Entry(context.Background(), "active", "01TEST")
	if err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if want := "https://api.example.com/v1/cart?a=1"; got.URL != want {
		t.Errorf("URL = %q, want %q", got.URL, want)
	}
}

// TestEntryRejectsUnusable covers the inputs that cannot become a request, so
// each fails with its own message instead of rendering something that lies.
func TestEntryRejectsUnusable(t *testing.T) {
	cases := map[string]struct {
		status int
		body   string
	}{
		"push promise":  {http.StatusOK, `{"id":"01TEST","transactionType":"push_promise","request":{"method":"GET","headers":[],"body":""}}`},
		"no method":     {http.StatusOK, `{"id":"01TEST","transactionType":"request","request":{"method":"","headers":[],"body":""}}`},
		"no url":        {http.StatusOK, `{"id":"01TEST","url":"","transactionType":"request","request":{"method":"GET","headers":[],"body":""}}`},
		"bad body":      {http.StatusOK, `{"id":"01TEST","url":"https://e.com/","transactionType":"request","request":{"method":"GET","headers":[],"body":"!!not base64!!"}}`},
		"unknown entry": {http.StatusNotFound, `not found`},
		"malformed":     {http.StatusOK, `{`},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := serve(t, c.status, c.body).Entry(context.Background(), "active", "01TEST"); err == nil {
				t.Error("Entry accepted an unusable payload, want an error")
			}
		})
	}

	if _, err := NewPowHTTP("http://127.0.0.1:1").Entry(context.Background(), "active", ""); err == nil {
		t.Error("Entry accepted an empty entry id, want an error")
	}
}

// TestNewPowHTTPResolvesBaseURL pins the precedence: an explicit address wins,
// then the environment, then the proxy's default.
func TestNewPowHTTPResolvesBaseURL(t *testing.T) {
	t.Setenv("POWHTTP_BASE_URL", "http://env:9999")
	if got := NewPowHTTP("http://explicit:1234/").BaseURL; got != "http://explicit:1234" {
		t.Errorf("explicit base URL = %q", got)
	}
	if got := NewPowHTTP("").BaseURL; got != "http://env:9999" {
		t.Errorf("env base URL = %q", got)
	}
	t.Setenv("POWHTTP_BASE_URL", "")
	if got := NewPowHTTP("").BaseURL; got != DefaultPowHTTPBaseURL {
		t.Errorf("default base URL = %q, want %q", got, DefaultPowHTTPBaseURL)
	}
}
