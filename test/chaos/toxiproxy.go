//go:build chaos

package chaos

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"
)

// toxiproxy is a minimal client of the Toxiproxy HTTP API (spec 11.6): it
// adds and removes toxics on the per-node proxies of deploy/toxiproxy.json
// and resets everything when healing. It talks to the API directly so the
// harness adds no dependency.
type toxiproxy struct {
	base string
	http *http.Client
}

func newToxiproxy(base string) *toxiproxy {
	return &toxiproxy{base: base, http: &http.Client{Timeout: 10 * time.Second}}
}

// toxic is the JSON body of POST /proxies/{proxy}/toxics.
type toxic struct {
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Stream     string         `json:"stream"`
	Toxicity   float64        `json:"toxicity"`
	Attributes map[string]any `json:"attributes"`
}

func (t *toxiproxy) do(ctx context.Context, method, path string, body any) ([]byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, t.base+path, rd)
	if err != nil {
		return nil, err
	}
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := t.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("toxiproxy %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return raw, fmt.Errorf("toxiproxy %s %s: status %d: %s", method, path, resp.StatusCode, bytes.TrimSpace(raw))
	}
	return raw, nil
}

// addToxic creates a toxic on the proxy; stream is upstream or downstream.
func (t *toxiproxy) addToxic(ctx context.Context, proxy, name, typ, stream string, attrs map[string]any) error {
	_, err := t.do(ctx, http.MethodPost, "/proxies/"+proxy+"/toxics", toxic{Name: name, Type: typ, Stream: stream, Toxicity: 1, Attributes: attrs})
	return err
}

// removeToxic deletes a toxic by name; a missing toxic is not an error.
func (t *toxiproxy) removeToxic(ctx context.Context, proxy, name string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, t.base+"/proxies/"+proxy+"/toxics/"+name, nil)
	if err != nil {
		return err
	}
	resp, err := t.http.Do(req)
	if err != nil {
		return fmt.Errorf("toxiproxy delete toxic %s/%s: %w", proxy, name, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("toxiproxy delete toxic %s/%s: status %d", proxy, name, resp.StatusCode)
	}
	return nil
}

// reset enables every proxy and removes every toxic (POST /reset).
func (t *toxiproxy) reset(ctx context.Context) error {
	_, err := t.do(ctx, http.MethodPost, "/reset", nil)
	return err
}

// proxies lists the proxy names.
func (t *toxiproxy) proxies(ctx context.Context) ([]string, error) {
	raw, err := t.do(ctx, http.MethodGet, "/proxies", nil)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("toxiproxy proxies: %w", err)
	}
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	return names, nil
}
