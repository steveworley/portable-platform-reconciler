package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/example/portable-platform-reconciler/internal/controller"
)

// statusSource is the read-only view the status server needs.
type statusSource interface {
	List() []controller.PolicyStatus
	Get(name string) (controller.PolicyStatus, bool)
}

func statusHandler(statuses statusSource) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		list := statuses.List()
		if list == nil {
			list = []controller.PolicyStatus{}
		}
		writeJSON(w, http.StatusOK, list)
	})
	mux.HandleFunc("GET /status/{name}", func(w http.ResponseWriter, r *http.Request) {
		st, ok := statuses.Get(r.PathValue("name"))
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no status for policy"})
			return
		}
		writeJSON(w, http.StatusOK, st)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func serveStatus(ctx context.Context, addr string, statuses *controller.StatusStore, logger *slog.Logger) error {
	srv := &http.Server{Addr: addr, Handler: statusHandler(statuses), ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("status server listening", "addr", addr)
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("status server: %w", err)
	case <-ctx.Done():
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutCtx)
}
