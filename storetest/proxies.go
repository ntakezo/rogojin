package storetest

import (
	"context"
	"testing"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/proxies"
)

// Proxies exercises the proxies.Repository contract: the shared leasing
// record behavior plus the model's own columns — URL and outcome stats.
func Proxies(t *testing.T, open func(t *testing.T) proxies.Repository) {
	ctx := context.Background()

	t.Run("Leasing", func(t *testing.T) {
		Leasing(t, open,
			func(id string) proxies.Proxy {
				return proxies.Proxy{Resource: leasing.Resource{ID: id}, URL: "http://" + id + ".example:8080"}
			},
			func(p *proxies.Proxy) *leasing.Resource { return &p.Resource })
	})

	// URL and stats round-trip whole, and a re-save lands their updates.
	t.Run("ModelFieldsRoundTrip", func(t *testing.T) {
		repo := open(t)
		p := proxies.Proxy{
			Resource:  leasing.Resource{ID: "p1", GroupID: "residential"},
			URL:       "http://p1.example:8080",
			Successes: 7, Failures: 2,
		}
		version, err := repo.Save(ctx, p)
		if err != nil {
			t.Fatalf("Save: %v", err)
		}

		listed, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		got := listed[0]
		if got.URL != "http://p1.example:8080" || got.Successes != 7 || got.Failures != 2 {
			t.Fatalf("record = %+v", got)
		}

		p.URL, p.Successes, p.Version = "http://new.example", 8, version
		if _, err := repo.Save(ctx, p); err != nil {
			t.Fatalf("second Save: %v", err)
		}
		listed, _ = repo.List(ctx)
		if len(listed) != 1 || listed[0].URL != "http://new.example" || listed[0].Successes != 8 {
			t.Fatalf("got %+v, want the replaced record alone", listed)
		}
	})
}
