package eventbridge

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/example/portable-platform-reconciler/internal/events"
)

// DefaultMaxBodyBytes is the default request body limit (1 MiB).
const DefaultMaxBodyBytes int64 = 1 << 20

// Response is the JSON body of a 202 response.
type Response struct {
	Keys []string `json:"keys"`
}

type handler struct {
	q        events.Enqueuer
	logger   *slog.Logger
	maxBytes int64
}

// HandlerOption configures NewHandler.
type HandlerOption func(*handler)

// WithMaxBodyBytes sets the maximum accepted request body size.
func WithMaxBodyBytes(n int64) HandlerOption {
	return func(h *handler) {
		if n > 0 {
			h.maxBytes = n
		}
	}
}

// NewHandler returns an http.Handler that accepts POSTed EventBridge events
// (one object or a JSON array) and enqueues the affected service keys. It
// responds 202 with the enqueued keys, 400 for a malformed body, 405 for other
// methods and 413 for an oversized body. Duplicate events enqueue the same key
// again; de-duplication is the queue's job. The handler never reconciles.
func NewHandler(q events.Enqueuer, logger *slog.Logger, opts ...HandlerOption) http.Handler {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	h := &handler{q: q, logger: logger, maxBytes: DefaultMaxBodyBytes}
	for _, o := range opts {
		o(h)
	}
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "read body: " + err.Error()})
		return
	}
	evs, err := decodeEvents(body)
	if err != nil {
		h.logger.Warn("rejecting malformed event", "error", err)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	keys := []string{}
	for _, ev := range evs {
		evKeys := KeysForEvent(ev)
		h.logger.Debug("event received", "id", ev.ID, "source", ev.Source, "detailType", ev.DetailType, "keys", evKeys)
		for _, k := range evKeys {
			h.q.Enqueue(k)
			keys = append(keys, k)
		}
	}
	writeJSON(w, http.StatusAccepted, Response{Keys: keys})
}

func decodeEvents(body []byte) ([]Event, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, errors.New("empty body")
	}
	if body[0] == '[' {
		var evs []Event
		if err := json.Unmarshal(body, &evs); err != nil {
			return nil, errors.New("invalid event array: " + err.Error())
		}
		return evs, nil
	}
	if body[0] != '{' {
		return nil, errors.New("body must be an event object or array")
	}
	var ev Event
	if err := json.Unmarshal(body, &ev); err != nil {
		return nil, errors.New("invalid event: " + err.Error())
	}
	return []Event{ev}, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
