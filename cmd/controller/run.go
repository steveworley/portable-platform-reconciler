package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/example/portable-platform-reconciler/internal/alerting"
	"github.com/example/portable-platform-reconciler/internal/config"
	"github.com/example/portable-platform-reconciler/internal/controller"
	"github.com/example/portable-platform-reconciler/internal/events"
	"github.com/example/portable-platform-reconciler/internal/events/eventbridge"
	k8ssource "github.com/example/portable-platform-reconciler/internal/events/kubernetes"
	"github.com/example/portable-platform-reconciler/internal/events/policyfile"
	"github.com/example/portable-platform-reconciler/internal/events/resync"
	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime"
	k8sruntime "github.com/example/portable-platform-reconciler/internal/runtime/kubernetes"

	ecsruntime "github.com/example/portable-platform-reconciler/internal/runtime/ecs"
)

// maxPolicyReload bounds how often the policy directory is re-read.
const maxPolicyReload = 30 * time.Second

func run(ctx context.Context, o options, logger *slog.Logger) error {
	cfg, err := config.Load(o.configPath, nil)
	if err != nil {
		return err
	}

	// Runtime is selected exactly once, at startup. Explicit config wins.
	rt, err := runtime.Select(cfg.Runtime, runtime.OSEnv)
	if err != nil {
		return fmt.Errorf("select runtime: %w", err)
	}
	adapter, err := runtime.New(rt, cfg, logger)
	if err != nil {
		return fmt.Errorf("create %s adapter: %w", rt, err)
	}

	// Fail fast on an invalid policy directory; the policyfile source keeps
	// the current policies if a later reload fails.
	validate := policy.WithTargetValidator(runtime.ValidateTarget)
	if _, err := policy.LoadDir(cfg.Policies.Dir, validate); err != nil {
		return fmt.Errorf("load policies: %w", err)
	}
	store := controller.NewMemoryPolicyStore()
	statuses := controller.NewStatusStore()

	var engineOpts []controller.Option
	engineOpts = append(engineOpts, controller.WithMaxRetries(cfg.Reconciliation.MaxRetries))
	backendName := "disabled"
	if cfg.Alerting.Enabled {
		backend, err := alerting.New(cfg.Alerting.Backend, cfg, logger)
		if err != nil {
			return fmt.Errorf("create alert backend: %w", err)
		}
		backendName = backend.Name()
		engineOpts = append(engineOpts, controller.WithSteps(alerting.NewStep(backend, alerting.WithLogger(logger))))
	}
	engine := controller.NewEngine(adapter, store, statuses, logger, engineOpts...)

	logger.Info("controller starting",
		"version", version,
		"runtime", string(rt),
		"alertBackend", backendName,
		"dryRun", dryRunSummary(cfg, rt),
		"resyncInterval", cfg.Reconciliation.ResyncInterval,
		"policiesDir", cfg.Policies.Dir,
	)

	resyncDur := cfg.ResyncDuration()
	sources, err := buildSources(cfg, rt, store, resyncDur, logger, validate)
	if err != nil {
		return err
	}

	g := newGroup(ctx)
	g.Go("engine", func(ctx context.Context) error { return engine.Run(ctx, cfg.Reconciliation.Workers) })
	for _, src := range sources {
		g.Go(src.Name(), func(ctx context.Context) error { return src.Start(ctx, engine) })
	}
	if cfg.Status.ListenAddr != "" {
		g.Go("status", func(ctx context.Context) error {
			return serveStatus(ctx, cfg.Status.ListenAddr, statuses, logger)
		})
	}
	err = g.Wait()
	logger.Info("controller stopped")
	return err
}

// dryRunSummary reports which parts of the controller only simulate or log
// their writes. "runtime" is true when runtime remediation is not really applied.
func dryRunSummary(cfg config.Config, rt runtime.RuntimeType) map[string]bool {
	m := map[string]bool{}
	switch rt {
	case k8sruntime.RuntimeType:
		m["runtime"] = !cfg.Kubernetes.ApplyAnnotations
	case ecsruntime.RuntimeType:
		m["runtime"] = true // ECS remediation is simulated or log-only in the POC
	}
	if cfg.Alerting.Enabled && cfg.Alerting.Backend == "newrelic" {
		m["alerts"] = cfg.Alerting.NewRelic.DryRun
	}
	return m
}

// buildSources returns the event sources for the selected runtime. All of
// them only enqueue; none reconcile.
func buildSources(cfg config.Config, rt runtime.RuntimeType, store *controller.MemoryPolicyStore, resyncDur time.Duration, logger *slog.Logger, validate policy.Option) ([]events.Source, error) {
	rs, err := resync.New(store, resyncDur, logger)
	if err != nil {
		return nil, err
	}
	reload := min(resyncDur, maxPolicyReload)
	pf, err := policyfile.New(cfg.Policies.Dir, reload, store, logger, validate)
	if err != nil {
		return nil, err
	}
	sources := []events.Source{rs, pf}

	switch rt {
	case k8sruntime.RuntimeType:
		rc, err := k8sruntime.RESTConfig(cfg.Kubernetes.Kubeconfig)
		if err != nil {
			return nil, err
		}
		client, err := kubernetes.NewForConfig(rc)
		if err != nil {
			return nil, fmt.Errorf("create kubernetes client: %w", err)
		}
		sources = append(sources, k8ssource.New(client, k8ssource.WithLogger(logger)))
	case ecsruntime.RuntimeType:
		sources = append(sources, eventbridge.NewSource(cfg.EventBridge.ListenAddr, eventbridge.WithLogger(logger)))
	}
	return sources, nil
}

// group runs goroutines until the parent context is cancelled or one fails; a
// failure cancels the rest. Wait returns the joined errors.
type group struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	errs   []error
}

func newGroup(parent context.Context) *group {
	ctx, cancel := context.WithCancel(parent)
	return &group{ctx: ctx, cancel: cancel}
}

// Go runs fn; an error (or an unexpected early nil return) stops the group.
func (g *group) Go(name string, fn func(ctx context.Context) error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		err := fn(g.ctx)
		if err != nil {
			g.mu.Lock()
			g.errs = append(g.errs, fmt.Errorf("%s: %w", name, err))
			g.mu.Unlock()
		}
		g.cancel()
	}()
}

// Wait blocks until every goroutine has returned.
func (g *group) Wait() error {
	g.wg.Wait()
	g.cancel()
	g.mu.Lock()
	defer g.mu.Unlock()
	return errors.Join(g.errs...)
}
