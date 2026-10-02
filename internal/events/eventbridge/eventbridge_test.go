package eventbridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/example/portable-platform-reconciler/internal/policy"
	"github.com/example/portable-platform-reconciler/internal/runtime/ecs"
)

const (
	svcARN     = "arn:aws:ecs:ap-southeast-2:123456789012:service/production/payments-api"
	clusterARN = "arn:aws:ecs:ap-southeast-2:123456789012:cluster/production"
	wantKey    = "ECSService:" + svcARN
)

type recorder struct {
	mu   sync.Mutex
	keys []string
}

func (r *recorder) Enqueue(k string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys = append(r.keys, k)
}

func (r *recorder) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.keys...)
}

func ev(detailType, source, detail string, resources ...string) Event {
	return Event{DetailType: detailType, Source: source, Resources: resources, Detail: json.RawMessage(detail)}
}

func TestKeysForEvent(t *testing.T) {
	tests := []struct {
		name string
		ev   Event
		want []string
	}{
		{name: "deployment with detail serviceArn", ev: ev(DetailTypeDeploymentChange, "aws.ecs", `{"clusterArn":"`+clusterARN+`","serviceArn":"`+svcARN+`"}`), want: []string{wantKey}},
		{name: "deployment with resources only", ev: ev(DetailTypeDeploymentChange, "aws.ecs", `{"eventName":"SERVICE_DEPLOYMENT_COMPLETED"}`, svcARN), want: []string{wantKey}},
		{name: "service action dedups detail and resources", ev: ev(DetailTypeServiceAction, "aws.ecs", `{"serviceArn":"`+svcARN+`"}`, svcARN), want: []string{wantKey}},
		{name: "service action ignores non-service resources", ev: ev(DetailTypeServiceAction, "aws.ecs", `{}`, clusterARN, "arn:aws:ecs:r:1:service/legacy"), want: nil},
		{name: "task state change", ev: ev(DetailTypeTaskChange, "aws.ecs", `{"clusterArn":"`+clusterARN+`","group":"service:payments-api","lastStatus":"STOPPED"}`), want: []string{wantKey}},
		{name: "task standalone group", ev: ev(DetailTypeTaskChange, "aws.ecs", `{"clusterArn":"`+clusterARN+`","group":"family:batch"}`), want: nil},
		{name: "task missing cluster", ev: ev(DetailTypeTaskChange, "aws.ecs", `{"group":"service:payments-api"}`), want: nil},
		{name: "non-ecs source", ev: ev(DetailTypeDeploymentChange, "aws.ec2", `{"serviceArn":"`+svcARN+`"}`), want: nil},
		{name: "unknown detail type", ev: ev("ECS Container Instance State Change", "aws.ecs", `{"serviceArn":"`+svcARN+`"}`), want: nil},
		{name: "malformed detail", ev: ev(DetailTypeDeploymentChange, "aws.ecs", `[1,2]`, svcARN), want: []string{wantKey}},
		{name: "empty", ev: Event{}, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := KeysForEvent(tt.ev); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("KeysForEvent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKeysMatchAdapterQueueKey(t *testing.T) {
	a := ecs.New(ecs.NewSimulatedECS())
	key, err := a.QueueKey(policy.TargetRef{Runtime: "ecs", Account: "123456789012", Region: "ap-southeast-2", Cluster: "production", Service: "payments-api"})
	if err != nil {
		t.Fatal(err)
	}
	got := KeysForEvent(ev(DetailTypeTaskChange, "aws.ecs", `{"clusterArn":"`+clusterARN+`","group":"service:payments-api"}`))
	if len(got) != 1 || got[0] != key {
		t.Errorf("KeysForEvent() = %v, want [%s]", got, key)
	}
}

func post(t *testing.T, h http.Handler, method, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, "/events", strings.NewReader(body)))
	return rec
}

func TestHandler(t *testing.T) {
	single := `{"source":"aws.ecs","detail-type":"ECS Deployment State Change","resources":["` + svcARN + `"],"detail":{}}`
	tests := []struct {
		name     string
		method   string
		body     string
		status   int
		wantKeys []string
	}{
		{name: "single", method: http.MethodPost, body: single, status: 202, wantKeys: []string{wantKey}},
		{name: "array", method: http.MethodPost, body: `[` + single + `,` + single + `]`, status: 202, wantKeys: []string{wantKey, wantKey}},
		{name: "ignored event", method: http.MethodPost, body: `{"source":"aws.s3","detail-type":"x"}`, status: 202},
		{name: "malformed", method: http.MethodPost, body: `{"source":`, status: 400},
		{name: "malformed array", method: http.MethodPost, body: `[1]`, status: 400},
		{name: "scalar", method: http.MethodPost, body: `42`, status: 400},
		{name: "empty", method: http.MethodPost, body: ``, status: 400},
		{name: "get", method: http.MethodGet, status: 405},
		{name: "put", method: http.MethodPut, body: single, status: 405},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			resp := post(t, NewHandler(rec, nil), tt.method, tt.body)
			if resp.Code != tt.status {
				t.Fatalf("status = %d, want %d (%s)", resp.Code, tt.status, resp.Body.String())
			}
			if got := rec.got(); !reflect.DeepEqual(got, tt.wantKeys) && (len(got) != 0 || len(tt.wantKeys) != 0) {
				t.Errorf("enqueued = %v, want %v", got, tt.wantKeys)
			}
			if tt.status == 202 {
				var body Response
				if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if len(body.Keys) != len(tt.wantKeys) {
					t.Errorf("response keys = %v, want %v", body.Keys, tt.wantKeys)
				}
			}
		})
	}
}

func TestHandlerDuplicateEventEnqueuesTwice(t *testing.T) {
	rec := &recorder{}
	h := NewHandler(rec, nil)
	body := `{"source":"aws.ecs","detail-type":"ECS Service Action","resources":["` + svcARN + `"]}`
	post(t, h, http.MethodPost, body)
	post(t, h, http.MethodPost, body)
	if got := rec.got(); !reflect.DeepEqual(got, []string{wantKey, wantKey}) {
		t.Errorf("enqueued = %v, want key twice", got)
	}
}

func TestHandlerBodyLimit(t *testing.T) {
	rec := &recorder{}
	h := NewHandler(rec, nil, WithMaxBodyBytes(16))
	resp := post(t, h, http.MethodPost, `{"source":"aws.ecs","detail-type":"ECS Service Action"}`)
	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", resp.Code)
	}
}

func TestSourceStartAndShutdown(t *testing.T) {
	rec := &recorder{}
	src := NewSource("127.0.0.1:0")
	if src.Name() != "eventbridge" {
		t.Errorf("Name() = %q", src.Name())
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- src.Start(ctx, rec) }()

	select {
	case <-src.Ready():
	case err := <-done:
		t.Fatalf("Start() returned early: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("source never became ready")
	}
	body := `{"source":"aws.ecs","detail-type":"ECS Deployment State Change","resources":["` + svcARN + `"]}`
	resp, err := http.Post("http://"+src.Addr()+EventsPath, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("status = %d, want 202", resp.StatusCode)
	}
	if got := rec.got(); !reflect.DeepEqual(got, []string{wantKey}) {
		t.Errorf("enqueued = %v", got)
	}
	if resp, err := http.Get("http://" + src.Addr() + "/other"); err == nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("/other status = %d, want 404", resp.StatusCode)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start() = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

func TestSourceListenError(t *testing.T) {
	if err := NewSource("256.0.0.1:99999").Start(context.Background(), &recorder{}); err == nil {
		t.Error("Start(bad addr) = nil error")
	}
}
