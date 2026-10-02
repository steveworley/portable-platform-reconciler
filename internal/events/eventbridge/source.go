package eventbridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/example/portable-platform-reconciler/internal/events"
)

// EventsPath is the HTTP path the source serves.
const EventsPath = "/events"

const shutdownTimeout = 5 * time.Second

// Source is an events.Source that serves EventBridge events over HTTP.
type Source struct {
	addr     string
	logger   *slog.Logger
	maxBytes int64

	ready chan struct{}
	mu    sync.Mutex
	bound string
}

var _ events.Source = (*Source)(nil)

// SourceOption configures NewSource.
type SourceOption func(*Source)

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) SourceOption {
	return func(s *Source) {
		if l != nil {
			s.logger = l
		}
	}
}

// WithSourceMaxBodyBytes sets the maximum request body size.
func WithSourceMaxBodyBytes(n int64) SourceOption { return func(s *Source) { s.maxBytes = n } }

// NewSource creates a source listening on addr (e.g. ":8080").
func NewSource(addr string, opts ...SourceOption) *Source {
	s := &Source{
		addr:     addr,
		logger:   slog.New(slog.DiscardHandler),
		maxBytes: DefaultMaxBodyBytes,
		ready:    make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Name implements events.Source.
func (s *Source) Name() string { return "eventbridge" }

// Ready is closed once the listener is bound.
func (s *Source) Ready() <-chan struct{} { return s.ready }

// Addr returns the bound listen address, or "" before Ready is closed. It is
// useful when addr used port 0.
func (s *Source) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bound
}

// Start serves until ctx is cancelled, then shuts down gracefully and returns nil.
func (s *Source) Start(ctx context.Context, q events.Enqueuer) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("eventbridge listen %s: %w", s.addr, err)
	}
	s.mu.Lock()
	s.bound = ln.Addr().String()
	s.mu.Unlock()
	close(s.ready)

	mux := http.NewServeMux()
	mux.Handle(EventsPath, NewHandler(q, s.logger, WithMaxBodyBytes(s.maxBytes)))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	s.logger.Info("eventbridge source listening", "addr", s.Addr(), "path", EventsPath)

	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("eventbridge serve: %w", err)
	case <-ctx.Done():
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		return fmt.Errorf("eventbridge shutdown: %w", err)
	}
	return nil
}
