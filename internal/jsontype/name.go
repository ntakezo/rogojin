package jsontype

import (
	"strconv"
	"strings"
)

// exportName names a struct field after its JSON key. It never returns the
// empty string — a key that yields no identifier still needs a field to bind
// through its tag, and lands on a placeholder instead.
func exportName(key string) string {
	if name := fieldIdent(key); name != "" {
		return name
	}
	return "Field"
}

// fieldIdent derives an exported Go field name from a JSON key, keeping the
// initialisms Go style capitalizes whole and running the components together as
// Go names do. It returns the empty string for a key that yields no identifier.
func fieldIdent(key string) string {
	var b strings.Builder
	for _, word := range SplitWords(key) {
		if initialisms[strings.ToUpper(word)] {
			b.WriteString(strings.ToUpper(word))
			continue
		}
		r := []rune(word)
		b.WriteString(strings.ToUpper(string(r[0])))
		b.WriteString(string(r[1:]))
	}
	out := b.String()
	if out == "" {
		return ""
	}
	if out[0] >= '0' && out[0] <= '9' {
		return "F" + out
	}
	return out
}

// initialisms are the abbreviations Go writes in full caps, so a generated field
// reads the way the same field would if it had been typed by hand.
var initialisms = map[string]bool{
	"API": true, "ASCII": true, "CSRF": true, "CSS": true, "DNS": true, "EOF": true,
	"GUID": true, "HTML": true, "HTTP": true, "HTTPS": true, "ID": true, "IP": true,
	"JSON": true, "JWT": true, "OK": true, "SKU": true, "SLA": true, "SQL": true,
	"SSO": true, "TLS": true, "TTL": true, "UI": true, "UID": true, "URI": true,
	"URL": true, "UTF8": true, "UUID": true, "XML": true,
}

// uniqueFieldName keeps two distinct JSON keys that reduce to one identifier
// from colliding, since a struct cannot declare the same field twice.
func uniqueFieldName(name string, taken map[string]bool) string {
	if !taken[name] {
		return name
	}
	for i := 2; ; i++ {
		candidate := name + strconv.Itoa(i)
		if !taken[candidate] {
			return candidate
		}
	}
}

// SplitWords breaks a raw name into words on any run of non-alphanumeric
// characters and on lower-to-upper transitions, so kebab, snake, and camel
// spellings of one name all split the same way.
func SplitWords(name string) []string {
	var words []string
	var cur []rune
	var prev rune
	for _, r := range name {
		alnum := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
		switch {
		case !alnum:
			if len(cur) > 0 {
				words = append(words, string(cur))
				cur = nil
			}
		default:
			if len(cur) > 0 && prev >= 'a' && prev <= 'z' && r >= 'A' && r <= 'Z' {
				words = append(words, string(cur))
				cur = nil
			}
			cur = append(cur, r)
		}
		prev = r
	}
	if len(cur) > 0 {
		words = append(words, string(cur))
	}
	return words
}
