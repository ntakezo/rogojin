// Package capture reads recorded HTTP exchanges from a capture tool and reduces
// them to what a generated request function needs: the URL, the wire order of
// the headers and HTTP/2 pseudo-headers, and the body bytes exactly as sent.
//
// The capture is the source of truth. Nothing here normalizes, sorts, or
// re-encodes what it reads — header order and body bytes are fingerprinting
// signals, and a value that looks wrong is a value the client really sent.
package capture

import "strings"

// Header is one header field as captured. Repeats are kept: HTTP/2 splits a
// cookie into several fields, and collapsing them changes the wire.
type Header struct {
	Name  string
	Value string
}

// Entry is one captured request, reduced to what rendering it needs. Headers
// excludes the pseudo-headers, which carry no value worth writing and are
// reproduced from Pseudo instead. Response is nil when the exchange recorded
// no reply.
type Entry struct {
	ID       string
	URL      string
	Method   string
	Proto    string
	Headers  []Header
	Pseudo   []string
	Body     []byte
	Response *Response
}

// Response is the captured reply, reduced to what typing it needs. MediaType is
// the content type with its parameters stripped, and Body is the payload the
// client saw: powhttp stores it already decoded, so a gzip or brotli response
// arrives as the plain bytes it decompressed to.
type Response struct {
	StatusCode int
	MediaType  string
	Body       []byte
}

// dropped are the headers a generated request must not write: the transport
// derives Content-Length from the body it is given, and a stale literal
// contradicts it.
var dropped = map[string]bool{"content-length": true}

// splitHeaders divides captured fields into the pseudo-header names and the
// ordinary headers, preserving the order of each and dropping only what the
// transport owns.
func splitHeaders(fields []Header) (headers []Header, pseudo []string) {
	for _, f := range fields {
		if strings.HasPrefix(f.Name, ":") {
			pseudo = append(pseudo, f.Name)
			continue
		}
		if dropped[strings.ToLower(f.Name)] {
			continue
		}
		headers = append(headers, f)
	}
	return headers, pseudo
}
