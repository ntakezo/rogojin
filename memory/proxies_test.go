package memory

import (
	"testing"

	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/storetest"
)

// TestProxiesContract runs the shared store contract against the in-memory
// proxies store.
func TestProxiesContract(t *testing.T) {
	storetest.Proxies(t, func(t *testing.T) proxies.Repository { return NewProxies() })
}
