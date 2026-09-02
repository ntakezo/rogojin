package redis

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/storetest"
	goredis "github.com/redis/go-redis/v9"
)

// miniClient opens a client on a fresh in-process miniredis, so the contract
// runs hermetically — CI needs no server.
func miniClient(t *testing.T) *goredis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { c.Close() })
	return c
}

// liveClient opens a client on the Redis named by ROGOJIN_REDIS_ADDR,
// skipping when unset — the same gate the postgres suite uses.
func liveClient(t *testing.T) *goredis.Client {
	t.Helper()
	addr := os.Getenv("ROGOJIN_REDIS_ADDR")
	if addr == "" {
		t.Skip("ROGOJIN_REDIS_ADDR not set; skipping the live-Redis suite")
	}
	c := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { c.Close() })
	return c
}

// uniquePrefix isolates each case's channels, so suites racing on a shared
// live server never hear each other.
func uniquePrefix(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("prefix: %v", err)
	}
	return "rogojin-test:" + hex.EncodeToString(raw) + ":"
}

func openBus(client func(t *testing.T) *goredis.Client) func(t *testing.T) comms.Bus {
	return func(t *testing.T) comms.Bus {
		b := NewBus(client(t), WithPrefix(uniquePrefix(t)))
		t.Cleanup(func() { b.Close() })
		return b
	}
}

func openNotifier(client func(t *testing.T) *goredis.Client) func(t *testing.T) comms.Notifier {
	return func(t *testing.T) comms.Notifier {
		n := NewNotifier(client(t), WithPrefix(uniquePrefix(t)))
		t.Cleanup(func() { n.Close() })
		return n
	}
}

// TestBusContract runs the shared transport contract hermetically.
func TestBusContract(t *testing.T) {
	storetest.Bus(t, openBus(miniClient))
}

// TestBusContractLive runs the same contract against a real server.
func TestBusContractLive(t *testing.T) {
	storetest.Bus(t, openBus(liveClient))
}

// TestNotifierContract runs the shared wakeup contract hermetically.
func TestNotifierContract(t *testing.T) {
	storetest.Notifier(t, openNotifier(miniClient))
}

// TestNotifierContractLive runs the same contract against a real server.
func TestNotifierContractLive(t *testing.T) {
	storetest.Notifier(t, openNotifier(liveClient))
}
