// Package memory is the in-memory persistence layer: one map-backed
// implementation of every repository port the framework defines — accounts,
// payments, proxies, email, and tasks — for embedded runs that want no file
// and for tests that want the real store semantics without one.
//
// Each store mirrors its sqlite counterpart's observable behavior exactly:
// the same sentinels, the same upsert-versus-strict-update split, the same
// created_at preservation, the same normalizations a round trip through
// columns produces. Records are deep-copied on the way in and out, so a
// caller mutating what it saved or listed never reaches the store's copy —
// the boundary serialization gives sqlite for free.
package memory

import (
	"encoding/json"
	"errors"
	"sort"
	"time"
)

// copyBytes clones a byte payload, keeping nil nil.
func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// copyFields mirrors the fields column round trip: empty stores as nil, and
// a payload that is not valid JSON is refused at the write.
func copyFields(fields json.RawMessage) (json.RawMessage, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	if !json.Valid(fields) {
		return nil, errors.New("fields are not valid JSON")
	}
	return json.RawMessage(copyBytes(fields)), nil
}

// copyMap mirrors the map column round trip: an empty map stores as nil.
func copyMap[K ~string](m map[K]string) map[K]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[K]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// storeTime normalizes a timestamp the way the text column does: UTC, with
// the zero time kept zero.
func storeTime(t time.Time) time.Time {
	if t.IsZero() {
		return time.Time{}
	}
	return t.UTC()
}

// sortedIDs returns the map's keys in stable id order.
func sortedIDs[V any](m map[string]V) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
