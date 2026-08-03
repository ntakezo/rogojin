package jsontype

import (
	"encoding/json"
	"time"
)

// value accumulates everything observed about one position in the document: a
// count per JSON kind, so merged observations can widen to the narrowest Go
// type that still holds them all, and — for objects — the order their keys
// first appeared in, which a type that is marshaled back onto the wire has to
// reproduce.
type value struct {
	observations int
	// empties counts the observations encoding/json's omitempty would drop —
	// "", false, an empty array or object. A field that was ever empty on the
	// wire never earns the tag: it would round-trip to a different body.
	empties  int
	arrays   int
	bools    int
	float64s int
	ints     int
	nulls    int
	objects  int
	strings  int
	// times counts strings that parsed as RFC 3339 — an implicit more specific
	// type than string, claimed only while every string observed qualifies.
	times int

	elems  *value            // array elements, merged across every element
	fields map[string]*value // object properties, merged across observations
	keys   []string          // property keys, in order of first appearance
}

// observeDocument folds the next JSON value from dec into v, allocating v on
// first observation. Walking tokens rather than unmarshaling is what lets one
// pass record both the kinds and the key order — a map would lose the latter.
func observeDocument(dec *json.Decoder, v *value) (*value, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if v == nil {
		v = &value{}
	}
	v.observations++
	switch tok := tok.(type) {
	case json.Delim:
		if tok == '{' {
			return v, v.observeObject(dec)
		}
		return v, v.observeArray(dec)
	case bool:
		v.bools++
		if !tok {
			v.empties++
		}
	case string:
		if tok == "" {
			v.empties++
		}
		if v.times == v.strings {
			if _, err := time.Parse(time.RFC3339Nano, tok); err == nil {
				v.times++
			}
		}
		v.strings++
	case json.Number:
		if _, err := tok.Int64(); err == nil {
			v.ints++
		} else {
			v.float64s++
		}
	case nil:
		v.nulls++
	}
	return v, nil
}

// observeObject folds an object's properties into v, key by key. A key seen
// across several observations merges into one field; a key seen for the first
// time is appended, so keys only later observations carried still land after
// the ones that came before them.
func (v *value) observeObject(dec *json.Decoder) error {
	v.objects++
	n := 0
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return err
		}
		key, _ := keyTok.(string)
		prev, seen := v.fields[key]
		child, err := observeDocument(dec, prev)
		if err != nil {
			return err
		}
		if v.fields == nil {
			v.fields = make(map[string]*value)
		}
		if !seen {
			v.keys = append(v.keys, key)
		}
		v.fields[key] = child
		n++
	}
	if _, err := dec.Token(); err != nil { // closing brace
		return err
	}
	if n == 0 {
		v.empties++
	}
	return nil
}

// observeArray folds every element into one merged element value, so an
// endpoint that varies its elements is typed from all of them rather than the
// first alone.
func (v *value) observeArray(dec *json.Decoder) error {
	v.arrays++
	if v.elems == nil {
		v.elems = &value{}
	}
	n := 0
	for dec.More() {
		elems, err := observeDocument(dec, v.elems)
		if err != nil {
			return err
		}
		v.elems = elems
		n++
	}
	if _, err := dec.Token(); err != nil { // closing bracket
		return err
	}
	if n == 0 {
		v.empties++
	}
	return nil
}
