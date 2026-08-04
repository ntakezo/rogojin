package capture

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// HAR is a parsed HAR 1.2 archive. Entries keep the file's order, which is the
// order they were recorded in, so an index identifies one stably.
type HAR struct {
	Name    string
	entries []harEntry
}

// harArchive mirrors the archive envelope. Only what a replay needs is read:
// timings and cache tell a client nothing, and the cookie arrays restate the
// headers in a form that has already lost their order.
type harArchive struct {
	Log struct {
		Entries []harEntry `json:"entries"`
	} `json:"log"`
}

// harEntry is one recorded exchange. ID reads the "_id" custom field some
// recorders write; the spec reserves the underscore prefix for exactly this.
type harEntry struct {
	ID      string `json:"_id"`
	Request struct {
		Method      string      `json:"method"`
		URL         string      `json:"url"`
		HTTPVersion string      `json:"httpVersion"`
		Headers     []harHeader `json:"headers"`
		PostData    *harPayload `json:"postData"`
	} `json:"request"`
	Response *struct {
		Status  int         `json:"status"`
		Headers []harHeader `json:"headers"`
		Content harPayload  `json:"content"`
	} `json:"response"`
}

// harPayload is a body in either direction. Encoding is "base64" when the
// recorder could not write the bytes as text; anything else means text is the
// body verbatim.
type harPayload struct {
	MimeType string      `json:"mimeType"`
	Text     string      `json:"text"`
	Encoding string      `json:"encoding"`
	Params   []harHeader `json:"params"`
}

type harHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ReadHAR parses the archive at path. The whole file is read up front: an entry
// is addressed by its position, which only the full list can resolve.
func ReadHAR(path string) (HAR, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return HAR{}, fmt.Errorf("har: read %s: %w", path, err)
	}
	var archive harArchive
	if err := json.Unmarshal(raw, &archive); err != nil {
		return HAR{}, fmt.Errorf("har: parse %s: %w", path, err)
	}
	if len(archive.Log.Entries) == 0 {
		return HAR{}, fmt.Errorf("har: %s holds no entries", path)
	}
	return HAR{Name: filepath.Base(path), entries: archive.Log.Entries}, nil
}

// Entry returns one recorded request. id is either the entry's "_id" or its
// 1-based position in the archive, so a HAR that names nothing is still
// addressable.
func (h HAR) Entry(id string) (Entry, error) {
	if id == "" {
		return Entry{}, fmt.Errorf("har: entry id is required\n%s", h.list())
	}
	for i, e := range h.entries {
		if e.ID != "" && e.ID == id {
			return e.normalize(h.source(i, e))
		}
	}
	if n, err := strconv.Atoi(id); err == nil && n >= 1 && n <= len(h.entries) {
		return h.entries[n-1].normalize(h.source(n-1, h.entries[n-1]))
	}
	return Entry{}, fmt.Errorf("har: no entry %q in %s\n%s", id, h.Name, h.list())
}

// source names the entry for the provenance comment the renderer writes.
func (h HAR) source(i int, e harEntry) string {
	if e.ID != "" {
		return fmt.Sprintf("HAR entry %s of %s", e.ID, h.Name)
	}
	return fmt.Sprintf("HAR entry %d of %s", i+1, h.Name)
}

// list renders the archive as addressable entries, so a wrong id answers with
// the ids that would have worked.
func (h HAR) list() string {
	ids := make([]string, len(h.entries))
	width := 0
	for i, e := range h.entries {
		ids[i] = strconv.Itoa(i + 1)
		if e.ID != "" {
			ids[i] = e.ID
		}
		width = max(width, len(ids[i]))
	}

	var b strings.Builder
	b.WriteString("entries in " + h.Name + ":\n")
	for i, e := range h.entries {
		fmt.Fprintf(&b, "  %-*s  %s %s\n", width, ids[i], e.Request.Method, e.Request.URL)
	}
	return strings.TrimRight(b.String(), "\n")
}

// normalize reduces a recorded exchange to the capture the renderer consumes,
// rejecting anything that is not a request a client could send again.
func (e harEntry) normalize(source string) (Entry, error) {
	if e.Request.Method == "" {
		return Entry{}, fmt.Errorf("har: %s has no request method", source)
	}
	if e.Request.URL == "" {
		return Entry{}, fmt.Errorf("har: %s has no URL to request", source)
	}

	fields := make([]Header, 0, len(e.Request.Headers))
	for _, h := range e.Request.Headers {
		fields = append(fields, Header{Name: h.Name, Value: h.Value})
	}
	headers, pseudo := splitHeaders(fields)

	body, err := e.requestBody(source)
	if err != nil {
		return Entry{}, err
	}

	return Entry{
		ID:       e.ID,
		Source:   source,
		URL:      e.Request.URL,
		Method:   e.Request.Method,
		Proto:    e.Request.HTTPVersion,
		Headers:  headers,
		Pseudo:   pseudo,
		Body:     body,
		Response: e.response(),
	}, nil
}

// requestBody decodes the recorded payload. A recorder that wrote only the
// parsed params fails the entry rather than re-encoding them: the order and
// escaping of a form body are the body, and rebuilding one guesses at both.
func (e harEntry) requestBody(source string) ([]byte, error) {
	if e.Request.PostData == nil {
		return nil, nil
	}
	post := *e.Request.PostData
	if post.Text == "" {
		if len(post.Params) == 0 {
			return nil, nil
		}
		return nil, fmt.Errorf("har: %s records its body only as parsed params, not as the bytes that were sent — re-record it with content", source)
	}
	body, err := harBody(post.Text, post.Encoding)
	if err != nil {
		return nil, fmt.Errorf("har: decode body of %s: %w", source, err)
	}
	return body, nil
}

// response reduces the recorded reply to what typing it needs. A status of zero
// is a request the recorder never saw answered. A body that will not decode is
// dropped rather than failing the entry: the request is still worth generating.
func (e harEntry) response() *Response {
	if e.Response == nil || e.Response.Status == 0 {
		return nil
	}
	out := &Response{StatusCode: e.Response.Status}
	if parsed, _, err := mime.ParseMediaType(e.Response.Content.MimeType); err == nil {
		out.MediaType = strings.ToLower(parsed)
	}
	if out.MediaType == "" {
		for _, h := range e.Response.Headers {
			if !strings.EqualFold(h.Name, "content-type") {
				continue
			}
			if parsed, _, err := mime.ParseMediaType(h.Value); err == nil {
				out.MediaType = strings.ToLower(parsed)
			}
			break
		}
	}
	if body, err := harBody(e.Response.Content.Text, e.Response.Content.Encoding); err == nil {
		out.Body = body
	}
	return out
}

// harBody decodes a recorded payload: base64 when the recorder says so,
// otherwise the text exactly as written.
func harBody(text, encoding string) ([]byte, error) {
	if text == "" {
		return nil, nil
	}
	if strings.EqualFold(encoding, "base64") {
		return base64.StdEncoding.DecodeString(text)
	}
	return []byte(text), nil
}
