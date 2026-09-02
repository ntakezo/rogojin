package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/leasing"
)

// checkpointSpy is a Deps.Checkpoint that counts calls and can fail.
type checkpointSpy struct {
	calls int
	err   error
}

func (s *checkpointSpy) fn(ctx context.Context) error {
	s.calls++
	return s.err
}

// recorderSpy is a Deps.RecordEffect that mimics the store: the first write
// per key wins, later calls read it back. err, when set, fails the record the
// way a store outage would.
type recorderSpy struct {
	stored map[string][]byte
	calls  int
	err    error
}

func (s *recorderSpy) fn(ctx context.Context, key string, result []byte) ([]byte, bool, error) {
	s.calls++
	if s.err != nil {
		return nil, false, s.err
	}
	if v, ok := s.stored[key]; ok {
		return v, false, nil
	}
	if s.stored == nil {
		s.stored = make(map[string][]byte)
	}
	s.stored[key] = append([]byte(nil), result...)
	return result, true, nil
}

// TestDoRunsEffectOncePersistsResult verifies the happy path: the effect runs,
// its result is recorded durably the moment it lands — with no checkpoint
// involved — and a second call returns the recorded result without re-running
// the effect or touching the store again.
func TestDoRunsEffectOncePersistsResult(t *testing.T) {
	spy := &checkpointSpy{}
	rec := &recorderSpy{}
	b := NewBase(Deps{Checkpoint: spy.fn, RecordEffect: rec.fn})
	effects := 0
	run := func() (string, error) {
		return Do(context.Background(), &b, "mint", func(ctx context.Context) (string, error) {
			effects++
			return "cookie-1", nil
		})
	}

	got, err := run()
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if got != "cookie-1" || effects != 1 || rec.calls != 1 {
		t.Fatalf("got=%q effects=%d records=%d, want cookie-1/1/1", got, effects, rec.calls)
	}
	if spy.calls != 0 {
		t.Fatalf("checkpoints = %d, want 0 — effect durability must not ride on a checkpoint", spy.calls)
	}

	got, err = run()
	if err != nil {
		t.Fatalf("Do replay: %v", err)
	}
	if got != "cookie-1" || effects != 1 || rec.calls != 1 {
		t.Fatalf("replay got=%q effects=%d records=%d, want cached cookie-1 with no new effect or record", got, effects, rec.calls)
	}
}

// TestDoDiscardsLoserOfARecordRace verifies the at-most-once contract under a
// racing duplicate run: when the store already holds another run's result for
// the key, Do discards the local result and returns the recorded one, because
// whatever built on the recorded result elsewhere must not be contradicted.
func TestDoDiscardsLoserOfARecordRace(t *testing.T) {
	rec := &recorderSpy{stored: map[string][]byte{"mint": []byte(`"cookie-theirs"`)}}
	b := NewBase(Deps{RecordEffect: rec.fn})

	got, err := Do(context.Background(), &b, "mint", func(ctx context.Context) (string, error) {
		return "cookie-ours", nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if got != "cookie-theirs" {
		t.Fatalf("got %q, want the recorded cookie-theirs", got)
	}

	// The replay serves the recorded result from the cache.
	got, err = Do(context.Background(), &b, "mint", func(ctx context.Context) (string, error) {
		t.Fatal("effect re-ran")
		return "", nil
	})
	if err != nil || got != "cookie-theirs" {
		t.Fatalf("replay got %q, %v; want cookie-theirs, nil", got, err)
	}
}

// TestDoFailedEffectRecordsNothing verifies a failed effect stays retryable:
// no record, no checkpoint, and the next call runs the effect again.
func TestDoFailedEffectRecordsNothing(t *testing.T) {
	spy := &checkpointSpy{}
	b := NewBase(Deps{Checkpoint: spy.fn})
	fail := errors.New("effect failed")
	effects := 0
	run := func() (int, error) {
		return Do(context.Background(), &b, "charge", func(ctx context.Context) (int, error) {
			effects++
			if effects == 1 {
				return 0, fail
			}
			return 42, nil
		})
	}

	if _, err := run(); !errors.Is(err, fail) {
		t.Fatalf("Do err = %v, want the effect failure", err)
	}
	if spy.calls != 0 {
		t.Fatalf("checkpoints = %d after failed effect, want 0", spy.calls)
	}
	got, err := run()
	if err != nil || got != 42 {
		t.Fatalf("retry got %d, %v; want 42, nil", got, err)
	}
	if effects != 2 {
		t.Fatalf("effects = %d, want 2", effects)
	}
}

// TestDoRecordErrorSurfacedCacheStays verifies a record failure is returned
// but the in-memory cache stays: the effect did happen, so a retry must not
// repeat it — yet Do keeps failing until the record lands, because a state
// must not report success while its effect's durability is still owed.
func TestDoRecordErrorSurfacedCacheStays(t *testing.T) {
	fail := errors.New("store down")
	rec := &recorderSpy{err: fail}
	b := NewBase(Deps{RecordEffect: rec.fn})
	effects := 0
	run := func() (string, error) {
		return Do(context.Background(), &b, "submit", func(ctx context.Context) (string, error) {
			effects++
			return "order-1", nil
		})
	}

	if _, err := run(); !errors.Is(err, fail) {
		t.Fatalf("Do err = %v, want the record failure", err)
	}
	// The store is still down: the retry skips the effect but fails again.
	if _, err := run(); !errors.Is(err, fail) {
		t.Fatalf("retry err = %v, want the record failure again", err)
	}
	if effects != 1 {
		t.Fatalf("effects = %d, want 1 — the cache must skip the effect", effects)
	}

	// The store recovers: the retry lands the record and succeeds.
	rec.err = nil
	got, err := run()
	if err != nil || got != "order-1" {
		t.Fatalf("replay got %q, %v; want the recorded order-1, nil", got, err)
	}
	if _, ok := rec.stored["submit"]; !ok {
		t.Fatal("record never landed after the store recovered")
	}
	if effects != 1 {
		t.Fatalf("effects = %d, want 1", effects)
	}
}

// TestOnceNilCheckpointTolerated verifies a Base built by hand — no checkpoint
// wired — runs the effect and records it without persisting.
func TestOnceNilCheckpointTolerated(t *testing.T) {
	b := NewBase(Deps{})
	effects := 0
	effect := func(ctx context.Context) error { effects++; return nil }
	if err := b.Once(context.Background(), "emit", effect); err != nil {
		t.Fatalf("Once with nil checkpoint: %v", err)
	}
	if err := b.Once(context.Background(), "emit", effect); err != nil {
		t.Fatalf("Once replay: %v", err)
	}
	if effects != 1 {
		t.Fatalf("effects = %d, want 1", effects)
	}
}

// TestSnapshotRoundTrip verifies the envelope carries input and registered
// durable state through Snapshot and back through restore, while the effect
// log rides the store: the snapshot no longer embeds it, and the seed the
// framework passes through Deps.Effects is what makes a rebuilt instance skip
// a recorded effect.
func TestSnapshotRoundTrip(t *testing.T) {
	type durable struct {
		Cookie string `json:"cookie"`
	}
	rec := &recorderSpy{}
	b := NewBase(Deps{})
	b.bind(Deps{RecordEffect: rec.fn}, json.RawMessage(`{"url":"https://example.com"}`))
	d := durable{Cookie: "queue-1"}
	b.Persist(&d)
	if err := b.Once(context.Background(), "emit", func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("Once: %v", err)
	}

	blob, err := b.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	var env envelope
	if err := json.Unmarshal(blob, &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.Effects != nil {
		t.Fatal("snapshot embeds the effect log; it lives in the store now")
	}
	restored := NewBase(Deps{})
	restored.bind(Deps{Effects: rec.stored}, env.Input)
	var d2 durable
	restored.Persist(&d2)
	if err := restored.restore(env); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if d2.Cookie != "queue-1" {
		t.Fatalf("restored cookie = %q, want queue-1", d2.Cookie)
	}
	effects := 0
	if err := restored.Once(context.Background(), "emit", func(ctx context.Context) error { effects++; return nil }); err != nil {
		t.Fatalf("Once on restored base: %v", err)
	}
	if effects != 0 {
		t.Fatal("restored effect log did not skip a recorded effect")
	}
}

// TestRestoreMigratesLegacyEffects verifies a snapshot from the envelope era
// still protects its effects: restore folds them into the cache and writes
// them through to the store eagerly, because the next checkpoint no longer
// carries them — left unmigrated, a crash after it would re-fire the effect.
func TestRestoreMigratesLegacyEffects(t *testing.T) {
	rec := &recorderSpy{}
	b := NewBase(Deps{})
	b.bind(Deps{RecordEffect: rec.fn}, nil)
	env := envelope{Effects: map[string]json.RawMessage{"emit": json.RawMessage(`{}`)}}
	if err := b.restore(env); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if _, ok := rec.stored["emit"]; !ok {
		t.Fatal("legacy effect was not written through to the store")
	}
	effects := 0
	if err := b.Once(context.Background(), "emit", func(ctx context.Context) error { effects++; return nil }); err != nil {
		t.Fatalf("Once: %v", err)
	}
	if effects != 0 {
		t.Fatal("legacy effect log did not skip the recorded effect")
	}

	// A store-seeded record for the same key outranks the envelope copy.
	seeded := NewBase(Deps{})
	seeded.bind(Deps{Effects: map[string][]byte{"emit": []byte(`{"v":"store"}`)}, RecordEffect: rec.fn}, nil)
	migrations := rec.calls
	if err := seeded.restore(env); err != nil {
		t.Fatalf("restore with seed: %v", err)
	}
	if rec.calls != migrations {
		t.Fatal("a store-seeded key was re-migrated from the envelope")
	}

	// With no store wired the envelope copy still serves the cache.
	bare := NewBase(Deps{})
	if err := bare.restore(env); err != nil {
		t.Fatalf("restore without store: %v", err)
	}
	effects = 0
	if err := bare.Once(context.Background(), "emit", func(ctx context.Context) error { effects++; return nil }); err != nil {
		t.Fatalf("Once without store: %v", err)
	}
	if effects != 0 {
		t.Fatal("storeless legacy restore did not skip the recorded effect")
	}
}

// TestRetryRetriesUntilSuccess verifies Retry re-runs a failing handler and
// returns the eventual success.
func TestRetryRetriesUntilSuccess(t *testing.T) {
	runs := 0
	h := Retry(5, ConstantBackoff(time.Millisecond))(func(ctx context.Context) (*State, error) {
		runs++
		if runs < 3 {
			return nil, errors.New("transient")
		}
		return Next("done"), nil
	})
	next, err := h(context.Background())
	if err != nil || next == nil || *next != "done" {
		t.Fatalf("handler = %v, %v; want done, nil", next, err)
	}
	if runs != 3 {
		t.Fatalf("runs = %d, want 3", runs)
	}
}

// TestRetryExhaustsAttempts verifies the last error surfaces after attempts
// total runs.
func TestRetryExhaustsAttempts(t *testing.T) {
	runs := 0
	fail := errors.New("still down")
	h := Retry(3, ConstantBackoff(0))(func(ctx context.Context) (*State, error) {
		runs++
		return nil, fail
	})
	if _, err := h(context.Background()); !errors.Is(err, fail) {
		t.Fatalf("err = %v, want the handler failure", err)
	}
	if runs != 3 {
		t.Fatalf("runs = %d, want 3", runs)
	}
}

// TestRetryStopsOnPermanent verifies a Permanent error short-circuits the
// remaining attempts and unwraps to the cause.
func TestRetryStopsOnPermanent(t *testing.T) {
	runs := 0
	cause := errors.New("sold out")
	h := Retry(5, ConstantBackoff(0))(func(ctx context.Context) (*State, error) {
		runs++
		return nil, fmt.Errorf("submit: %w", Permanent(cause))
	})
	if _, err := h(context.Background()); !errors.Is(err, cause) {
		t.Fatalf("err = %v, want the permanent cause", err)
	}
	if runs != 1 {
		t.Fatalf("runs = %d, want 1", runs)
	}
}

// TestRetryHonorsCancellation verifies a canceled context stops the retry
// loop with the handler's error instead of sleeping through the backoff.
func TestRetryHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runs := 0
	fail := errors.New("transient")
	h := Retry(3, ConstantBackoff(time.Hour))(func(c context.Context) (*State, error) {
		runs++
		cancel()
		return nil, fail
	})
	done := make(chan error, 1)
	go func() {
		_, err := h(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, fail) {
			t.Fatalf("err = %v, want the handler failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retry slept through cancellation")
	}
	if runs != 1 {
		t.Fatalf("runs = %d, want 1", runs)
	}
}

// TestExpBackoffGrowsAndCaps verifies the geometric growth and the cap.
func TestExpBackoffGrowsAndCaps(t *testing.T) {
	b := ExpBackoff(100*time.Millisecond, 2, time.Second)
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second, time.Second}
	for i, w := range want {
		if got := b(i + 1); got != w {
			t.Fatalf("backoff(%d) = %v, want %v", i+1, got, w)
		}
	}
}

// TestTimeoutBoundsHandler verifies the deadline reaches the handler's context.
func TestTimeoutBoundsHandler(t *testing.T) {
	h := Timeout(time.Millisecond)(func(ctx context.Context) (*State, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if _, err := h(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
}

// TestOnAppliesOptionsInOrder verifies later options wrap earlier ones: with
// Timeout inside Retry, every attempt gets a fresh deadline, so the retry
// recovers from a first attempt that timed out.
func TestOnAppliesOptionsInOrder(t *testing.T) {
	runs := 0
	def := On("s", func(ctx context.Context) (*State, error) {
		runs++
		if runs == 1 {
			<-ctx.Done() // burn the first attempt's deadline
			return nil, ctx.Err()
		}
		return nil, nil
	}, Timeout(10*time.Millisecond), Retry(2, ConstantBackoff(0)))
	g := NewGraph("s", def)
	if _, err := g.Handler("s")(context.Background()); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if runs != 2 {
		t.Fatalf("runs = %d, want a timed-out attempt plus a fresh one", runs)
	}
}

// moduleInstance is the test instance: Base plus one durable counter.
type moduleInstance struct {
	Base
	in testInput
	d  moduleDurable
}

type testInput struct {
	URL string `json:"url"`
}

type moduleDurable struct {
	Visited int `json:"visited"`
}

func newModuleInstance(in testInput) *moduleInstance {
	c := &moduleInstance{in: in}
	c.Persist(&c.d)
	return c
}

func (c *moduleInstance) Graph() Graph {
	return NewGraph("s", On("s", func(ctx context.Context) (*State, error) { return nil, nil }))
}

func testModule() *Module[testInput] {
	return NewModule("test", func(in testInput, deps Deps) (Instance, error) {
		return newModuleInstance(in), nil
	})
}

// TestModuleValidateInput verifies the type check, the nil-input zero value,
// and the WithValidate hook.
func TestModuleValidateInput(t *testing.T) {
	m := testModule()
	if err := m.ValidateInput(testInput{URL: "x"}); err != nil {
		t.Fatalf("ValidateInput: %v", err)
	}
	if err := m.ValidateInput(nil); err != nil {
		t.Fatalf("ValidateInput(nil): %v", err)
	}
	if err := m.ValidateInput(42); err == nil {
		t.Fatal("ValidateInput accepted a mistyped input")
	}
	m.WithValidate(func(in testInput) error {
		if in.URL == "" {
			return errors.New("url required")
		}
		return nil
	})
	if err := m.ValidateInput(testInput{}); err == nil {
		t.Fatal("WithValidate hook did not run")
	}
}

// TestModuleBindsDeps verifies the built instance sees the task's deps through
// its Base accessors.
func TestModuleBindsDeps(t *testing.T) {
	inst, err := testModule().NewInstance(testInput{URL: "x"}, Deps{TaskID: "task-1"})
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	c := inst.(*moduleInstance)
	if c.TaskID() != "task-1" {
		t.Fatalf("TaskID = %q, want task-1", c.TaskID())
	}
}

// TestModuleRestoreRoundTrip verifies a snapshot rebuilds the instance with
// its input and durable state intact, and that the store's effect log —
// carried in through Deps.Effects — makes the rebuilt instance skip a
// recorded effect.
func TestModuleRestoreRoundTrip(t *testing.T) {
	m := testModule()
	rec := &recorderSpy{}
	inst, err := m.NewInstance(testInput{URL: "https://example.com"}, Deps{TaskID: "task-1", RecordEffect: rec.fn})
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	c := inst.(*moduleInstance)
	c.d.Visited = 3
	if err := c.Once(context.Background(), "emit", func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("Once: %v", err)
	}
	blob, err := c.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored, err := m.RestoreInstance(Deps{TaskID: "task-1", Effects: rec.stored}, blob)
	if err != nil {
		t.Fatalf("RestoreInstance: %v", err)
	}
	r := restored.(*moduleInstance)
	if r.in.URL != "https://example.com" || r.d.Visited != 3 {
		t.Fatalf("restored input=%q visited=%d, want the recorded values", r.in.URL, r.d.Visited)
	}
	effects := 0
	if err := r.Once(context.Background(), "emit", func(ctx context.Context) error { effects++; return nil }); err != nil {
		t.Fatalf("Once on restored instance: %v", err)
	}
	if effects != 0 {
		t.Fatal("restored effect log did not skip the recorded effect")
	}
}

// TestModuleRefusesBaselessInstance verifies the loud failure when a build
// function returns an instance that does not embed Base.
func TestModuleRefusesBaselessInstance(t *testing.T) {
	m := NewModule("baseless", func(in testInput, deps Deps) (Instance, error) {
		return baselessInstance{}, nil
	})
	if _, err := m.NewInstance(testInput{}, Deps{}); err == nil {
		t.Fatal("NewInstance accepted an instance without a Base")
	}
}

type baselessInstance struct{}

func (baselessInstance) Graph() Graph { return NewGraph("s") }

// TestResource verifies the typed extraction and both failure shapes.
func TestResource(t *testing.T) {
	type mgr struct{ name string }
	managers := map[leasing.Kind]any{"proxies": &mgr{name: "px"}}
	got, err := Resource[*mgr](managers, "proxies")
	if err != nil || got.name != "px" {
		t.Fatalf("Resource = %v, %v; want the registered manager", got, err)
	}
	if _, err := Resource[*mgr](managers, "accounts"); err == nil {
		t.Fatal("Resource found a manager under a missing kind")
	}
	if _, err := Resource[string](managers, "proxies"); err == nil {
		t.Fatal("Resource accepted a mistyped manager")
	}
}
