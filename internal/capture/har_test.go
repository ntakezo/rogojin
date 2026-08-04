package capture

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeHAR puts an archive on disk and parses it, so each case exercises the
// adapter the way the CLI reaches it.
func writeHAR(t *testing.T, body string) HAR {
	t.Helper()
	path := filepath.Join(t.TempDir(), "capture.har")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	archive, err := ReadHAR(path)
	if err != nil {
		t.Fatalf("ReadHAR: %v", err)
	}
	return archive
}

// harJSON wraps entry bodies in the archive envelope.
func harJSON(entries ...string) string {
	return `{"log":{"version":"1.2","entries":[` + strings.Join(entries, ",") + `]}}`
}

// TestHAREntryReadsCapturedOrder is the contract every adapter holds: wire order
// survives the read, pseudo-headers are split out of the ordinary ones, and
// nothing else is dropped.
func TestHAREntryReadsCapturedOrder(t *testing.T) {
	entry := `{"_id":"add-to-cart","request":{"method":"POST","url":"https://shop.example.com/cart","httpVersion":"HTTP/2",
	  "headers":[{"name":":method","value":"POST"},{"name":":authority","value":"shop.example.com"},
	    {"name":":scheme","value":"https"},{"name":":path","value":"/cart"},
	    {"name":"content-length","value":"29"},{"name":"content-type","value":"application/x-www-form-urlencoded"},
	    {"name":"cookie","value":"a=1"},{"name":"cookie","value":"b=2"}],
	  "postData":{"mimeType":"application/x-www-form-urlencoded","text":"variantID=variant-M&quantity=1"}}}`

	got, err := writeHAR(t, harJSON(entry)).Entry("add-to-cart")
	if err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if want := "https://shop.example.com/cart"; got.URL != want {
		t.Errorf("URL = %q, want %q", got.URL, want)
	}
	if got.Method != "POST" {
		t.Errorf("Method = %q, want POST", got.Method)
	}
	if want := "variantID=variant-M&quantity=1"; string(got.Body) != want {
		t.Errorf("Body = %q, want %q", got.Body, want)
	}

	wantPseudo := []string{":method", ":authority", ":scheme", ":path"}
	if strings.Join(got.Pseudo, ",") != strings.Join(wantPseudo, ",") {
		t.Errorf("Pseudo = %v, want %v", got.Pseudo, wantPseudo)
	}

	var order []string
	for _, h := range got.Headers {
		order = append(order, h.Name)
	}
	wantOrder := []string{"content-length", "content-type", "cookie", "cookie"}
	if strings.Join(order, ",") != strings.Join(wantOrder, ",") {
		t.Errorf("header order = %v, want %v", order, wantOrder)
	}
}

// TestHAREntryAddressing covers both ways an entry is named: the "_id" a
// recorder may write, and the position of one that carries none.
func TestHAREntryAddressing(t *testing.T) {
	first := `{"request":{"method":"GET","url":"https://e.com/one","headers":[]}}`
	second := `{"_id":"named","request":{"method":"GET","url":"https://e.com/two","headers":[]}}`
	archive := writeHAR(t, harJSON(first, second))

	for id, want := range map[string]string{
		"1":     "https://e.com/one",
		"2":     "https://e.com/two",
		"named": "https://e.com/two",
	} {
		got, err := archive.Entry(id)
		if err != nil {
			t.Fatalf("Entry(%q): %v", id, err)
		}
		if got.URL != want {
			t.Errorf("Entry(%q).URL = %q, want %q", id, got.URL, want)
		}
	}

	// A wrong id has to answer with the ids that would have worked, or the file
	// has to be read by hand to find one.
	_, err := archive.Entry("nope")
	if err == nil {
		t.Fatal("Entry accepted an unknown id, want an error")
	}
	for _, want := range []string{"named", "https://e.com/one"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not list %q:\n%v", want, err)
		}
	}
}

// TestHAREntryProvenance pins that the entry names its own origin: the renderer
// cites it verbatim and must not have to know a HAR from a proxy session.
func TestHAREntryProvenance(t *testing.T) {
	named := `{"_id":"checkout","request":{"method":"GET","url":"https://e.com/","headers":[]}}`
	got, err := writeHAR(t, harJSON(named)).Entry("checkout")
	if err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if want := "HAR entry checkout of capture.har"; got.Source != want {
		t.Errorf("Source = %q, want %q", got.Source, want)
	}

	anon := `{"request":{"method":"GET","url":"https://e.com/","headers":[]}}`
	got, err = writeHAR(t, harJSON(anon)).Entry("1")
	if err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if want := "HAR entry 1 of capture.har"; got.Source != want {
		t.Errorf("Source = %q, want %q", got.Source, want)
	}
}

// TestHAREntryReadsResponse covers the reply side: the media type is stripped of
// its parameters, and a base64 payload arrives decoded.
func TestHAREntryReadsResponse(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte(`{"ok":true}`))
	entry := `{"request":{"method":"GET","url":"https://e.com/","headers":[]},
	  "response":{"status":201,"headers":[],
	    "content":{"mimeType":"application/json; charset=utf-8","text":"` + encoded + `","encoding":"base64"}}}`

	got, err := writeHAR(t, harJSON(entry)).Entry("1")
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

// TestHAREntryToleratesUnusableResponse keeps a reply that cannot be read from
// costing us the request, which is the half worth generating either way. A
// status of zero is a request the recorder never saw answered.
func TestHAREntryToleratesUnusableResponse(t *testing.T) {
	for name, response := range map[string]string{
		"absent":      `null`,
		"unanswered":  `{"status":0,"headers":[],"content":{"mimeType":"","text":""}}`,
		"no body":     `{"status":204,"headers":[],"content":{"mimeType":"application/json","text":""}}`,
		"bad base64":  `{"status":200,"headers":[],"content":{"mimeType":"application/json","text":"!!!","encoding":"base64"}}`,
		"typed later": `{"status":200,"headers":[{"name":"Content-Type","value":"application/json; charset=utf-8"}],"content":{"mimeType":"","text":"{}"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			entry := `{"request":{"method":"GET","url":"https://e.com/","headers":[]},"response":` + response + `}`
			got, err := writeHAR(t, harJSON(entry)).Entry("1")
			if err != nil {
				t.Fatalf("an unreadable response failed the whole entry: %v", err)
			}
			if got.URL != "https://e.com/" {
				t.Errorf("request was lost with the response: URL = %q", got.URL)
			}
		})
	}

	// A recorder that dropped the content type still leaves it on the headers,
	// which is the only place a response body can be typed from.
	entry := `{"request":{"method":"GET","url":"https://e.com/","headers":[]},
	  "response":{"status":200,"headers":[{"name":"Content-Type","value":"application/json; charset=utf-8"}],"content":{"mimeType":"","text":"{}"}}}`
	got, err := writeHAR(t, harJSON(entry)).Entry("1")
	if err != nil {
		t.Fatalf("Entry: %v", err)
	}
	if got.Response.MediaType != "application/json" {
		t.Errorf("MediaType = %q, want application/json from the header", got.Response.MediaType)
	}
}

// TestHARRejectsUnusable covers the inputs that cannot become a request, so each
// fails with its own message instead of rendering something that lies. A body
// recorded only as parsed params is the one worth calling out: re-encoding it
// would guess at the order and escaping that are the body.
func TestHARRejectsUnusable(t *testing.T) {
	for name, body := range map[string]string{
		"malformed":   `{`,
		"no entries":  harJSON(),
		"no method":   harJSON(`{"request":{"method":"","url":"https://e.com/","headers":[]}}`),
		"no url":      harJSON(`{"request":{"method":"GET","url":"","headers":[]}}`),
		"params only": harJSON(`{"request":{"method":"POST","url":"https://e.com/","headers":[],"postData":{"mimeType":"application/x-www-form-urlencoded","text":"","params":[{"name":"a","value":"1"}]}}}`),
		"bad base64":  harJSON(`{"request":{"method":"POST","url":"https://e.com/","headers":[],"postData":{"mimeType":"application/json","text":"!!!","encoding":"base64"}}}`),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "capture.har")
			if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			archive, err := ReadHAR(path)
			if err != nil {
				return // rejected at parse, which is as good as rejected at read
			}
			if _, err := archive.Entry("1"); err == nil {
				t.Error("Entry accepted an unusable payload, want an error")
			}
		})
	}

	if _, err := ReadHAR(filepath.Join(t.TempDir(), "missing.har")); err == nil {
		t.Error("ReadHAR accepted a missing file, want an error")
	}
	archive := writeHAR(t, harJSON(`{"request":{"method":"GET","url":"https://e.com/","headers":[]}}`))
	if _, err := archive.Entry(""); err == nil {
		t.Error("Entry accepted an empty id, want an error")
	}
}

// TestExampleHARIsUsable reads the archive the checkout example is generated
// from, so a change to it that the scaffolder could not consume fails here.
func TestExampleHARIsUsable(t *testing.T) {
	archive, err := ReadHAR(filepath.Join("..", "..", "_examples", "captures", "checkout.har"))
	if err != nil {
		t.Fatalf("ReadHAR: %v", err)
	}
	for _, id := range []string{"get-homepage", "add-to-cart", "submit-checkout"} {
		got, err := archive.Entry(id)
		if err != nil {
			t.Fatalf("Entry(%q): %v", id, err)
		}
		if got.Response == nil || len(got.Response.Body) == 0 {
			t.Errorf("entry %q has no response body to type from", id)
		}
		if len(got.Pseudo) != 4 {
			t.Errorf("entry %q carries %d pseudo-headers, want the full 4", id, len(got.Pseudo))
		}
	}
}
