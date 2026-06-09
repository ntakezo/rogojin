package proxies

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Bayesian selects by Thompson sampling: each candidate draws
// theta ~ Beta(1+Successes, 1+Failures) and the highest draw wins, trading
// exploitation of proven proxies against exploration of uncertain ones.
// It is safe for concurrent use.
type Bayesian struct {
	mu  sync.Mutex
	rng *rand.Rand
}

type BayesianOption func(*Bayesian)

// WithRand injects the random source, making selection deterministic for tests.
func WithRand(rng *rand.Rand) BayesianOption {
	return func(b *Bayesian) { b.rng = rng }
}

// WithSeed seeds the default random source.
func WithSeed(seed int64) BayesianOption {
	return func(b *Bayesian) { b.rng = rand.New(rand.NewSource(seed)) }
}

func NewBayesian(opts ...BayesianOption) *Bayesian {
	b := &Bayesian{rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Select draws a Beta sample per candidate and returns the highest.
func (b *Bayesian) Select(candidates []Proxy) (Proxy, error) {
	if len(candidates) == 0 {
		return Proxy{}, errors.New("no candidates")
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	best, bestTheta := Proxy{}, math.Inf(-1)
	for _, p := range candidates {
		theta := b.sampleBeta(float64(p.Successes)+1, float64(p.Failures)+1)
		if theta > bestTheta {
			best, bestTheta = p, theta
		}
	}
	return best, nil
}

// sampleBeta draws Beta(alpha, beta) as Ga/(Ga+Gb) from two gamma samples.
func (b *Bayesian) sampleBeta(alpha, beta float64) float64 {
	x := b.sampleGamma(alpha)
	y := b.sampleGamma(beta)
	return x / (x + y)
}

// sampleGamma draws Gamma(shape, 1) for shape >= 1 via Marsaglia-Tsang.
func (b *Bayesian) sampleGamma(shape float64) float64 {
	d := shape - 1.0/3.0
	c := 1.0 / math.Sqrt(9.0*d)
	for {
		x := b.rng.NormFloat64()
		v := 1.0 + c*x
		if v <= 0 {
			continue
		}
		v = v * v * v
		u := b.rng.Float64()
		if u < 1.0-0.0331*x*x*x*x {
			return d * v
		}
		if math.Log(u) < 0.5*x*x+d*(1.0-v+math.Log(v)) {
			return d * v
		}
	}
}
