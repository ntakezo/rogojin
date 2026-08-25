package proxies

import "github.com/ntakezo/rogojin/leasing"

// RoundRobin selects candidates in turn, spreading load evenly by advancing a
// cursor on every selection. It is safe for concurrent use.
type RoundRobin struct {
	core *leasing.RoundRobin[attrs]
}

// NewRoundRobin returns a RoundRobin with its cursor at the first candidate.
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{core: leasing.NewRoundRobin[attrs]()}
}

// Select returns the candidate at the cursor and advances it.
func (r *RoundRobin) Select(candidates []Proxy) (Proxy, error) {
	return selectVia(r.core, candidates)
}

// selectVia runs a core strategy over proxy-shaped candidates. Round robin is
// the one algorithm the leasing core owns — it is the default every resource
// kind falls back to — so this package translates rather than reimplements it.
func selectVia(core leasing.Selection[attrs], candidates []Proxy) (Proxy, error) {
	resources := make([]leasing.Resource[attrs], len(candidates))
	for i, p := range candidates {
		resources[i] = toResource(p)
	}
	picked, err := core.Select(resources)
	if err != nil {
		return Proxy{}, err
	}
	return fromResource(picked), nil
}
