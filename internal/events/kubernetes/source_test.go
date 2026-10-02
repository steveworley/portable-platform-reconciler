package kubernetes

import (
	"context"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type recorder struct {
	mu   sync.Mutex
	keys []string
}

func (r *recorder) Enqueue(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys = append(r.keys, key)
}

func (r *recorder) has(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, k := range r.keys {
		if k == key {
			return true
		}
	}
	return false
}

func (r *recorder) count(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, k := range r.keys {
		if k == key {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func dep(ns, name string) *appsv1.Deployment {
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
}

func TestSourceEnqueuesOnChanges(t *testing.T) {
	tests := []struct {
		name        string
		namespace   string
		act         func(ctx context.Context, t *testing.T, c *fake.Clientset)
		wantKey     string
		wantAtLeast int
	}{
		{
			name: "create",
			act: func(ctx context.Context, t *testing.T, c *fake.Clientset) {
				if _, err := c.AppsV1().Deployments("payments").Create(ctx, dep("payments", "api"), metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			},
			wantKey: "KubernetesDeployment:payments/api", wantAtLeast: 1,
		},
		{
			name:      "namespace scoped create",
			namespace: "payments",
			act: func(ctx context.Context, t *testing.T, c *fake.Clientset) {
				if _, err := c.AppsV1().Deployments("payments").Create(ctx, dep("payments", "api"), metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			},
			wantKey: "KubernetesDeployment:payments/api", wantAtLeast: 1,
		},
		{
			name: "update",
			act: func(ctx context.Context, t *testing.T, c *fake.Clientset) {
				if _, err := c.AppsV1().Deployments("payments").Create(ctx, dep("payments", "api"), metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				d := dep("payments", "api")
				d.Annotations = map[string]string{"x": "y"}
				if _, err := c.AppsV1().Deployments("payments").Update(ctx, d, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			},
			wantKey: "KubernetesDeployment:payments/api", wantAtLeast: 2,
		},
		{
			name: "delete",
			act: func(ctx context.Context, t *testing.T, c *fake.Clientset) {
				if _, err := c.AppsV1().Deployments("payments").Create(ctx, dep("payments", "api"), metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				if err := c.AppsV1().Deployments("payments").Delete(ctx, "api", metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			},
			wantKey: "KubernetesDeployment:payments/api", wantAtLeast: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewSimpleClientset()
			rec := &recorder{}
			var opts []Option
			if tt.namespace != "" {
				opts = append(opts, WithNamespace(tt.namespace))
			}
			src := New(c, opts...)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- src.Start(ctx, rec) }()
			// Give the informer time to establish its watch before mutating.
			time.Sleep(200 * time.Millisecond)
			tt.act(ctx, t, c)
			waitFor(t, "enqueue", func() bool { return rec.count(tt.wantKey) >= tt.wantAtLeast })
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Start() = %v, want nil", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("Start did not return after cancel")
			}
		})
	}
}

func TestSourceNamespaceScopeIgnoresOthers(t *testing.T) {
	c := fake.NewSimpleClientset()
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = New(c, WithNamespace("payments")).Start(ctx, rec) }()
	time.Sleep(200 * time.Millisecond)
	if _, err := c.AppsV1().Deployments("other").Create(ctx, dep("other", "x"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AppsV1().Deployments("payments").Create(ctx, dep("payments", "api"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "payments key", func() bool { return rec.has("KubernetesDeployment:payments/api") })
	if rec.has("KubernetesDeployment:other/x") {
		t.Error("enqueued key from out-of-scope namespace")
	}
}

func TestName(t *testing.T) {
	if got := New(fake.NewSimpleClientset()).Name(); got != "kubernetes" {
		t.Errorf("Name() = %q, want kubernetes", got)
	}
}
