package workflows

import (
	"context"
	"errors"
	"math"
	"time"
)

// A StateOption decorates a state's handler with a cross-cutting policy —
// retries, timeouts — at graph declaration, so the mechanism lives in the
// graph and the handler body stays the workflow's own logic.
type StateOption func(StateHandler) StateHandler

// A Backoff maps a completed attempt number (1-based) to the delay before the
// next attempt.
type Backoff func(attempt int) time.Duration

// ConstantBackoff waits d between every attempt.
func ConstantBackoff(d time.Duration) Backoff {
	return func(int) time.Duration { return d }
}

// ExpBackoff grows the delay geometrically: base after the first attempt,
// multiplied by factor after each further one, capped at max.
func ExpBackoff(base time.Duration, factor float64, max time.Duration) Backoff {
	return func(attempt int) time.Duration {
		d := time.Duration(float64(base) * math.Pow(factor, float64(attempt-1)))
		if d <= 0 || d > max { // <= 0 catches overflow past the cap
			return max
		}
		return d
	}
}

// Retry re-runs a failed handler up to attempts total runs, waiting
// backoff(attempt) between them. It stops early on a context cancellation or
// an error marked Permanent, returning the underlying error. A retry re-runs
// the whole handler, so an effect a re-run must not repeat belongs in Do or
// Once — its record makes the re-run skip the effect while the rest of the
// state retries.
//
// The attempt count is in-process: a crash mid-state recovers into a fresh
// count, not a continued one.
func Retry(attempts int, backoff Backoff) StateOption {
	return func(h StateHandler) StateHandler {
		return func(ctx context.Context) (*State, error) {
			for attempt := 1; ; attempt++ {
				next, err := h(ctx)
				if err == nil {
					return next, nil
				}
				var perm *permanentError
				if errors.As(err, &perm) {
					return nil, perm.Unwrap()
				}
				if attempt >= attempts || ctx.Err() != nil {
					return nil, err
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(backoff(attempt)):
				}
			}
		}
	}
}

// Timeout bounds the handler with a deadline. Options apply in order, later
// ones outermost: Retry then Timeout puts one deadline over all attempts;
// Timeout then Retry gives each attempt its own.
func Timeout(d time.Duration) StateOption {
	return func(h StateHandler) StateHandler {
		return func(ctx context.Context) (*State, error) {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return h(ctx)
		}
	}
}

// Permanent marks err as not worth retrying — a response that will not change
// on a re-run. Retry stops immediately and returns the underlying error.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }
