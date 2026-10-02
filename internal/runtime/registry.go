package runtime

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/example/portable-platform-reconciler/internal/config"
	"github.com/example/portable-platform-reconciler/internal/policy"
)

// Env looks up an environment variable. os.Getenv satisfies it.
type Env func(key string) string

// OSEnv reads the real process environment.
var OSEnv Env = os.Getenv

// Factory builds an adapter from the controller configuration.
// logger may be nil.
type Factory func(cfg config.Config, logger *slog.Logger) (RuntimeAdapter, error)

// Detector reports whether the process appears to be running on its runtime.
type Detector func(env Env) bool

// ErrNoRuntime is returned by Select when auto-detection matches nothing.
var ErrNoRuntime = errors.New("cannot detect runtime: set runtime.type explicitly")

// ErrAmbiguousRuntime is returned by Select when several detectors match.
var ErrAmbiguousRuntime = errors.New("multiple runtimes detected: set runtime.type explicitly")

// ErrUnknownRuntime is returned for a runtime type that is not registered.
var ErrUnknownRuntime = errors.New("unknown runtime")

type registration struct {
	factory  Factory
	detector Detector
	validate func(policy.TargetRef) error
}

// RegisterOption customises a registration.
type RegisterOption func(*registration)

// WithTargetValidator sets the runtime-specific target validation for a runtime.
func WithTargetValidator(fn func(policy.TargetRef) error) RegisterOption {
	return func(r *registration) { r.validate = fn }
}

// Registry maps runtime types to adapter factories and detectors. Adding a
// platform is one adapter package that registers itself; nothing else changes.
type Registry struct {
	mu      sync.RWMutex
	entries map[RuntimeType]registration
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{entries: map[RuntimeType]registration{}} }

// Default is the process-wide registry that adapters register into from init().
var Default = NewRegistry()

// Register adds a runtime. The detector may be nil, in which case the runtime
// is only selectable explicitly. Registering a type twice panics (programmer error).
func (r *Registry) Register(t RuntimeType, factory Factory, detector Detector, opts ...RegisterOption) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t == "" || factory == nil {
		panic("runtime: Register requires a type and a factory")
	}
	if _, dup := r.entries[t]; dup {
		panic(fmt.Sprintf("runtime: %q registered twice", t))
	}
	reg := registration{factory: factory, detector: detector}
	for _, opt := range opts {
		opt(&reg)
	}
	r.entries[t] = reg
}

// Types returns the registered runtime types, sorted.
func (r *Registry) Types() []RuntimeType {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RuntimeType, 0, len(r.entries))
	for t := range r.entries {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Select picks the runtime once at startup. An explicit configured type wins
// (and must be registered); "auto" or empty iterates the registered detectors
// and requires exactly one match.
func (r *Registry) Select(cfg config.RuntimeConfig, env Env) (RuntimeType, error) {
	if t := strings.ToLower(cfg.Type); t != "" && t != config.RuntimeAuto {
		rt := RuntimeType(t)
		r.mu.RLock()
		_, ok := r.entries[rt]
		r.mu.RUnlock()
		if !ok {
			return "", fmt.Errorf("%w %q (registered: %v)", ErrUnknownRuntime, cfg.Type, r.Types())
		}
		return rt, nil
	}
	if env == nil {
		env = OSEnv
	}
	var matches []RuntimeType
	for _, t := range r.Types() {
		r.mu.RLock()
		det := r.entries[t].detector
		r.mu.RUnlock()
		if det != nil && det(env) {
			matches = append(matches, t)
		}
	}
	switch len(matches) {
	case 0:
		return "", ErrNoRuntime
	case 1:
		return matches[0], nil
	}
	return "", fmt.Errorf("%w: %v", ErrAmbiguousRuntime, matches)
}

// New builds the adapter for t. A nil logger discards logs.
func (r *Registry) New(t RuntimeType, cfg config.Config, logger *slog.Logger) (RuntimeAdapter, error) {
	r.mu.RLock()
	reg, ok := r.entries[t]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownRuntime, t)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return reg.factory(cfg, logger)
}

// ValidateTarget runs the runtime-specific validation for target.Runtime.
// Use it with policy.WithTargetValidator.
func (r *Registry) ValidateTarget(target policy.TargetRef) error {
	r.mu.RLock()
	reg, ok := r.entries[RuntimeType(target.Runtime)]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w %q", ErrUnknownRuntime, target.Runtime)
	}
	if reg.validate == nil {
		return nil
	}
	return reg.validate(target)
}

// Register adds a runtime to the Default registry.
func Register(t RuntimeType, factory Factory, detector Detector, opts ...RegisterOption) {
	Default.Register(t, factory, detector, opts...)
}

// Select selects from the Default registry.
func Select(cfg config.RuntimeConfig, env Env) (RuntimeType, error) { return Default.Select(cfg, env) }

// New builds an adapter from the Default registry.
func New(t RuntimeType, cfg config.Config, logger *slog.Logger) (RuntimeAdapter, error) {
	return Default.New(t, cfg, logger)
}

// ValidateTarget validates a target against the Default registry.
func ValidateTarget(target policy.TargetRef) error { return Default.ValidateTarget(target) }

// EnvSet returns a Detector that matches when the variable is non-empty.
func EnvSet(key string) Detector { return func(env Env) bool { return env(key) != "" } }

// EnvHasPrefix returns a Detector that matches when the variable starts with prefix.
func EnvHasPrefix(key, prefix string) Detector {
	return func(env Env) bool { return strings.HasPrefix(env(key), prefix) }
}

// AnyOf returns a Detector that matches when any of ds matches.
func AnyOf(ds ...Detector) Detector {
	return func(env Env) bool {
		for _, d := range ds {
			if d(env) {
				return true
			}
		}
		return false
	}
}
