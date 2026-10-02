package kubernetes

import (
	"fmt"
	"log/slog"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/example/portable-platform-reconciler/internal/config"
	"github.com/example/portable-platform-reconciler/internal/runtime"
)

func init() {
	runtime.Register(RuntimeType, factory, runtime.EnvSet("KUBERNETES_SERVICE_HOST"),
		runtime.WithTargetValidator(ValidateTarget))
}

func factory(cfg config.Config) (runtime.RuntimeAdapter, error) {
	rc, err := restConfig(cfg.Kubernetes.Kubeconfig)
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	return New(client,
		WithLogger(slog.Default()),
		WithApplyAnnotations(cfg.Kubernetes.ApplyAnnotations),
	), nil
}

// RESTConfig builds a client configuration: an explicit kubeconfig path wins,
// then in-cluster config, then the default kubeconfig loading rules
// ($KUBECONFIG, ~/.kube/config). The event source reuses it.
func RESTConfig(kubeconfig string) (*rest.Config, error) { return restConfig(kubeconfig) }

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		rc, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("load kubeconfig %q: %w", kubeconfig, err)
		}
		return rc, nil
	}
	if rc, err := rest.InClusterConfig(); err == nil {
		return rc, nil
	}
	rc, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubernetes config: %w", err)
	}
	return rc, nil
}
