// Package kubernetes is an events.Source that watches Deployments through a
// shared informer and enqueues their queue keys.
package kubernetes

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/example/portable-platform-reconciler/internal/events"
	k8sruntime "github.com/example/portable-platform-reconciler/internal/runtime/kubernetes"
)

// Source watches apps/v1 Deployments. Handlers only compute
// DeploymentKey(ns, name) and call Enqueue; no reconciliation happens here and
// no object state is passed on.
//
// The informer relists and rewatches after a disconnect, but events can still
// be missed (e.g. during an outage or relist gaps coalescing changes).
// Correctness relies on the engine's periodic resync, not on watch continuity.
type Source struct {
	client    kubernetes.Interface
	logger    *slog.Logger
	namespace string
	resync    time.Duration
}

var _ events.Source = (*Source)(nil)

// Option configures a Source.
type Option func(*Source)

// WithLogger sets the logger. Default discards.
func WithLogger(l *slog.Logger) Option {
	return func(s *Source) {
		if l != nil {
			s.logger = l
		}
	}
}

// WithNamespace scopes the watch to one namespace. Default is all namespaces.
func WithNamespace(ns string) Option { return func(s *Source) { s.namespace = ns } }

// WithResyncPeriod sets the informer resync period (re-delivers Update events
// for cached objects). Default 0 (disabled); the engine's own resync suffices.
func WithResyncPeriod(d time.Duration) Option { return func(s *Source) { s.resync = d } }

// New creates a Source.
func New(client kubernetes.Interface, opts ...Option) *Source {
	s := &Source{client: client, logger: slog.New(slog.DiscardHandler)}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Name implements events.Source.
func (s *Source) Name() string { return "kubernetes" }

// Start implements events.Source. It blocks until ctx is done and returns nil,
// or returns an error if the cache fails to sync.
func (s *Source) Start(ctx context.Context, q events.Enqueuer) error {
	var factoryOpts []informers.SharedInformerOption
	if s.namespace != "" {
		factoryOpts = append(factoryOpts, informers.WithNamespace(s.namespace))
	}
	factory := informers.NewSharedInformerFactoryWithOptions(s.client, s.resync, factoryOpts...)
	informer := factory.Apps().V1().Deployments().Informer()

	enqueue := func(obj any) {
		d, ok := obj.(*appsv1.Deployment)
		if !ok {
			// Delete may deliver a tombstone when the watch missed the delete.
			tomb, isTomb := obj.(cache.DeletedFinalStateUnknown)
			if !isTomb {
				s.logger.Warn("unexpected object type", "type", fmt.Sprintf("%T", obj))
				return
			}
			if d, ok = tomb.Obj.(*appsv1.Deployment); !ok {
				s.logger.Warn("unexpected tombstone object type", "type", fmt.Sprintf("%T", tomb.Obj))
				return
			}
		}
		q.Enqueue(k8sruntime.DeploymentKey(d.Namespace, d.Name))
	}
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    enqueue,
		UpdateFunc: func(_, newObj any) { enqueue(newObj) },
		DeleteFunc: enqueue,
	}); err != nil {
		return fmt.Errorf("add event handler: %w", err)
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("deployment informer cache failed to sync")
	}
	s.logger.Info("kubernetes deployment watch started", "namespace", s.namespace)
	<-ctx.Done()
	factory.Shutdown()
	return nil
}
