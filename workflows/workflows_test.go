package workflows

import (
	"context"
	"errors"
	"testing"
)

// TestOnceRunsEffectAndPersists verifies the happy path: the effect runs, the
// guard is set before the checkpoint persists it, and a second call skips
// both — the whole point of the guard.
func TestOnceRunsEffectAndPersists(t *testing.T) {
	var done bool
	effects, checkpoints := 0, 0
	var guardAtCheckpoint bool
	checkpoint := func(ctx context.Context) error {
		checkpoints++
		guardAtCheckpoint = done
		return nil
	}
	effect := func(ctx context.Context) error {
		effects++
		return nil
	}

	if err := Once(context.Background(), &done, checkpoint, effect); err != nil {
		t.Fatalf("Once: %v", err)
	}
	if effects != 1 || checkpoints != 1 {
		t.Fatalf("effects=%d checkpoints=%d, want 1/1", effects, checkpoints)
	}
	if !guardAtCheckpoint {
		t.Fatal("guard was not set before the checkpoint persisted it")
	}

	if err := Once(context.Background(), &done, checkpoint, effect); err != nil {
		t.Fatalf("Once with guard up: %v", err)
	}
	if effects != 1 || checkpoints != 1 {
		t.Fatalf("effects=%d checkpoints=%d after guarded call, want still 1/1", effects, checkpoints)
	}
}

// TestOnceFailedEffectLeavesGuardDown verifies a failed effect stays
// retryable: the guard is not set, the checkpoint not written, and the next
// call runs the effect again.
func TestOnceFailedEffectLeavesGuardDown(t *testing.T) {
	var done bool
	effects := 0
	fail := errors.New("effect failed")
	effect := func(ctx context.Context) error {
		effects++
		if effects == 1 {
			return fail
		}
		return nil
	}
	checkpoint := func(ctx context.Context) error { return nil }

	if err := Once(context.Background(), &done, checkpoint, effect); !errors.Is(err, fail) {
		t.Fatalf("Once err = %v, want the effect failure", err)
	}
	if done {
		t.Fatal("guard set despite a failed effect")
	}
	if err := Once(context.Background(), &done, checkpoint, effect); err != nil {
		t.Fatalf("Once retry: %v", err)
	}
	if effects != 2 || !done {
		t.Fatalf("effects=%d done=%v after retry, want 2/true", effects, done)
	}
}

// TestOnceCheckpointErrorSurfacedGuardStays verifies a checkpoint failure is
// returned but the in-memory guard stays up: the effect did happen, so an
// in-process retry of the state must not repeat it.
func TestOnceCheckpointErrorSurfacedGuardStays(t *testing.T) {
	var done bool
	effects := 0
	fail := errors.New("store down")
	if err := Once(context.Background(), &done,
		func(ctx context.Context) error { return fail },
		func(ctx context.Context) error { effects++; return nil },
	); !errors.Is(err, fail) {
		t.Fatalf("Once err = %v, want the checkpoint failure", err)
	}
	if !done {
		t.Fatal("guard down despite a succeeded effect")
	}
	if effects != 1 {
		t.Fatalf("effects = %d, want 1", effects)
	}
}

// TestOnceNilCheckpointTolerated verifies a nil checkpoint — a Deps built by
// hand — runs the effect and sets the guard without persisting.
func TestOnceNilCheckpointTolerated(t *testing.T) {
	var done bool
	effects := 0
	if err := Once(context.Background(), &done, nil,
		func(ctx context.Context) error { effects++; return nil },
	); err != nil {
		t.Fatalf("Once with nil checkpoint: %v", err)
	}
	if effects != 1 || !done {
		t.Fatalf("effects=%d done=%v, want 1/true", effects, done)
	}
}
