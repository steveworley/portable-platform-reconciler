package policy

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"sigs.k8s.io/yaml"
)

// Parse decodes and validates a single policy document. Unknown fields are rejected.
func Parse(data []byte, opts ...Option) (ObservabilityPolicy, error) {
	var p ObservabilityPolicy
	if err := yaml.UnmarshalStrict(data, &p); err != nil {
		return ObservabilityPolicy{}, fmt.Errorf("decode policy: %w", err)
	}
	if p.Metadata.Generation == 0 {
		p.Metadata.Generation = 1
	}
	if err := Validate(p, opts...); err != nil {
		return ObservabilityPolicy{}, err
	}
	return p, nil
}

// LoadFile reads and parses one policy file.
func LoadFile(path string, opts ...Option) (ObservabilityPolicy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ObservabilityPolicy{}, fmt.Errorf("read policy: %w", err)
	}
	p, err := Parse(data, opts...)
	if err != nil {
		return ObservabilityPolicy{}, fmt.Errorf("%s: %w", path, err)
	}
	return p, nil
}

// LoadDir loads every *.yaml and *.yml file directly in dir, sorted by
// path. Duplicate policy names are an error.
func LoadDir(dir string, opts ...Option) ([]ObservabilityPolicy, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read policy dir: %w", err)
	}
	var paths []string
	for _, e := range entries {
		if ext := filepath.Ext(e.Name()); !e.IsDir() && (ext == ".yaml" || ext == ".yml") {
			paths = append(paths, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(paths)

	var out []ObservabilityPolicy
	seen := map[string]string{}
	for _, path := range paths {
		p, err := LoadFile(path, opts...)
		if err != nil {
			return nil, err
		}
		if prev, dup := seen[p.Metadata.Name]; dup {
			return nil, fmt.Errorf("%s: duplicate policy name %q (also in %s)", path, p.Metadata.Name, prev)
		}
		seen[p.Metadata.Name] = path
		out = append(out, p)
	}
	return out, nil
}

// Option customises validation.
type Option func(*options)

type options struct {
	targetValidator func(TargetRef) error
}

// WithTargetValidator adds runtime-specific target validation, typically
// runtime.ValidateTarget, so this package stays free of platform knowledge.
func WithTargetValidator(fn func(TargetRef) error) Option {
	return func(o *options) { o.targetValidator = fn }
}

// Validate checks the generic required fields and, if configured via
// WithTargetValidator, the runtime-specific target fields.
func Validate(p ObservabilityPolicy, opts ...Option) error {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	var errs []error
	if p.APIVersion != APIVersion {
		errs = append(errs, fmt.Errorf("apiVersion must be %q, got %q", APIVersion, p.APIVersion))
	}
	if p.Kind != Kind {
		errs = append(errs, fmt.Errorf("kind must be %q, got %q", Kind, p.Kind))
	}
	if p.Metadata.Name == "" {
		errs = append(errs, errors.New("metadata.name is required"))
	}
	if p.Target.Runtime == "" {
		errs = append(errs, errors.New("target.runtime is required"))
	} else if o.targetValidator != nil {
		if err := o.targetValidator(p.Target); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
