package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/example/portable-platform-reconciler/internal/alerting"
	"github.com/example/portable-platform-reconciler/internal/controller"
	"github.com/example/portable-platform-reconciler/internal/events"
	"github.com/example/portable-platform-reconciler/internal/events/resync"
	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	"github.com/example/portable-platform-reconciler/internal/state"

	// Register runtimes (target validators) and alert backends.
	_ "github.com/example/portable-platform-reconciler/internal/alerting/memory"
	_ "github.com/example/portable-platform-reconciler/internal/alerting/newrelic"
	_ "github.com/example/portable-platform-reconciler/internal/runtime/ecs"
	_ "github.com/example/portable-platform-reconciler/internal/runtime/kubernetes"
)

// env carries output, logging and shared settings for the scenarios.
type env struct {
	mu       sync.Mutex
	out      io.Writer
	logger   *slog.Logger
	examples string
	resync   time.Duration
	fails    []string
	closers  []func()
	shared   *ecsStack // ECS stack shared by scenarios A, B and C
}

func newEnv(out io.Writer, logger *slog.Logger, examples string, resync time.Duration) *env {
	return &env{out: out, logger: logger, examples: examples, resync: resync}
}

func (e *env) close() {
	for i := len(e.closers) - 1; i >= 0; i-- {
		e.closers[i]()
	}
}

func (e *env) printf(format string, args ...any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	fmt.Fprintf(e.out, format, args...)
}

func (e *env) header(id, title string) {
	e.printf("\n=== Scenario %s: %s ===\n", id, title)
}

func (e *env) fail(msg string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.fails = append(e.fails, msg)
}

func (e *env) failures() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.fails)
}

// check prints a PASS/FAIL line and records failures.
func (e *env) check(ok bool, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if ok {
		e.printf("  [PASS] %s\n", msg)
		return
	}
	e.printf("  [FAIL] %s\n", msg)
	e.fail(msg)
}

// findExamples walks up from the working directory to find examples/.
func findExamples() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		cand := filepath.Join(dir, "examples")
		if _, err := os.Stat(filepath.Join(cand, "ecs", "ecs-policy.yaml")); err == nil {
			return cand, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("cannot find examples/ecs/ecs-policy.yaml; run from the repository or pass --examples")
		}
		dir = parent
	}
}

func (e *env) loadPolicy(rel string) (policy.ObservabilityPolicy, error) {
	return policy.LoadFile(filepath.Join(e.examples, rel), policy.WithTargetValidator(runtime.ValidateTarget))
}

// harness is a running engine (real Engine.Run with workers) plus event
// sources, with every reconcile outcome recorded and echoed.
type harness struct {
	env      *env
	label    string
	adapter  runtime.RuntimeAdapter
	store    *controller.MemoryPolicyStore
	statuses *controller.StatusStore
	engine   *controller.Engine

	mu   sync.Mutex
	outs []controller.Outcome

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newHarness(e *env, label string, adapter runtime.RuntimeAdapter, backend alerting.Backend) *harness {
	h := &harness{
		env:      e,
		label:    label,
		adapter:  adapter,
		store:    controller.NewMemoryPolicyStore(),
		statuses: controller.NewStatusStore(),
	}
	h.engine = controller.NewEngine(adapter, h.store, h.statuses, e.logger,
		controller.WithBackoff(20*time.Millisecond, time.Second),
		controller.WithObserver(h.observe),
		controller.WithSteps(alerting.NewStep(backend, alerting.WithLogger(e.logger))),
	)
	return h
}

func (h *harness) observe(o controller.Outcome) {
	h.mu.Lock()
	h.outs = append(h.outs, o)
	h.mu.Unlock()
	acts := make([]string, len(o.Actions))
	for i, a := range o.Actions {
		acts[i] = string(a.Name)
	}
	extra := ""
	if o.Err != nil {
		extra = " err=" + o.Err.Error()
	}
	h.env.printf("    ~ [%s] reconcile %s: noop=%t phase=%s actions=%d%s\n",
		h.label, shortKey(o.Key), o.NoOp, o.Phase, len(acts), extra)
}

func shortKey(k string) string {
	typ, rest, _ := strings.Cut(k, ":")
	if i := strings.LastIndex(rest, "/"); i >= 0 {
		rest = rest[i+1:]
	}
	return typ + ":" + rest
}

// start runs the engine and the resync source plus any extra sources.
func (h *harness) start(ctx context.Context, resyncEvery time.Duration, sources ...events.Source) error {
	rs, err := resync.New(h.store, resyncEvery, h.env.logger)
	if err != nil {
		return err
	}
	ctx, h.cancel = context.WithCancel(ctx)
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		_ = h.engine.Run(ctx, 2)
	}()
	for _, src := range append([]events.Source{rs}, sources...) {
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			if err := src.Start(ctx, h.engine); err != nil {
				h.env.logger.Error("event source failed", "source", src.Name(), "error", err)
			}
		}()
	}
	h.env.closers = append(h.env.closers, h.stop)
	return nil
}

func (h *harness) stop() {
	if h.cancel != nil {
		h.cancel()
		h.wg.Wait()
		h.cancel = nil
	}
}

func (h *harness) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.outs)
}

// outcomesFrom returns outcomes recorded after index from whose key has the prefix.
func (h *harness) outcomesFrom(from int, keyPrefix string) []controller.Outcome {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []controller.Outcome
	for _, o := range h.outs[from:] {
		if strings.HasPrefix(o.Key, keyPrefix) {
			out = append(out, o)
		}
	}
	return out
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// alertsReconciled reports whether the policy's AlertsReconciled condition is True.
func (h *harness) alertsReconciled(name string) bool {
	st, ok := h.statuses.Get(name)
	if !ok {
		return false
	}
	c, ok := st.Condition(alerting.ConditionAlertsReconciled)
	return ok && c.Status == controller.ConditionTrue
}

func hasAction(o controller.Outcome, name state.ActionName) bool {
	for _, a := range o.Actions {
		if a.Name == name {
			return true
		}
	}
	return false
}
