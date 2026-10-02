package newrelic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// NerdGraph endpoints per region.
const (
	EndpointUS = "https://api.newrelic.com/graphql"
	EndpointEU = "https://api.eu.newrelic.com/graphql"
)

// Client wraps NerdGraph. Query returns the response's "data" member.
type Client interface {
	Query(ctx context.Context, gql string, vars map[string]any) (json.RawMessage, error)
}

// HTTPClient is the real NerdGraph client.
type HTTPClient struct {
	endpoint string
	apiKey   string
	hc       *http.Client
}

var _ Client = (*HTTPClient)(nil)

// NewHTTPClient returns a client that POSTs to endpoint with the API-Key
// header. A nil hc gets a client with a 30s timeout.
func NewHTTPClient(endpoint, apiKey string, hc *http.Client) *HTTPClient {
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	return &HTTPClient{endpoint: endpoint, apiKey: apiKey, hc: hc}
}

// EndpointFor returns the NerdGraph endpoint for region "US" or "EU".
func EndpointFor(region string) (string, error) {
	switch strings.ToUpper(region) {
	case "", "US":
		return EndpointUS, nil
	case "EU":
		return EndpointEU, nil
	}
	return "", fmt.Errorf("unknown New Relic region %q", region)
}

// Query implements Client.
func (c *HTTPClient) Query(ctx context.Context, gql string, vars map[string]any) (json.RawMessage, error) {
	body, err := json.Marshal(map[string]any{"query": gql, "variables": vars})
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("API-Key", c.apiKey)
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("nerdgraph request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("read nerdgraph response: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("nerdgraph status %d: %s", resp.StatusCode, truncate(string(raw), 300))
	}
	var out struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode nerdgraph response: %w", err)
	}
	if len(out.Errors) > 0 {
		msgs := make([]string, len(out.Errors))
		for i, e := range out.Errors {
			msgs[i] = e.Message
		}
		return nil, errors.New("nerdgraph errors: " + strings.Join(msgs, "; "))
	}
	return out.Data, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
