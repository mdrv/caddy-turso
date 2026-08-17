package caddyturso

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// QueryDef is a named SQL statement with param bindings and output config.
type QueryDef struct {
	Name     string         `json:"name"`
	SQL      string         `json:"sql"`
	Params   []ParamBinding `json:"params,omitempty"`
	Output   OutputConfig   `json:"output,omitempty"`
	CacheTTL int64          `json:"cache_ttl_ns,omitempty"`
	Timeout  int64          `json:"timeout_ns,omitempty"`
}

type ParamBinding struct {
	Name    string   `json:"name"`
	Source  string   `json:"source"` // query|header|body|path|env|placeholder
	Key     string   `json:"key"`
	Type    string   `json:"type,omitempty"`    // int|float|bool|string
	Default *string  `json:"default,omitempty"` // declared default; *Default == "" means "empty string", not NULL
	Min     *float64 `json:"min,omitempty"`
	Max     *float64 `json:"max,omitempty"`
	Cap     *float64 `json:"cap,omitempty"`
	Pattern string   `json:"pattern,omitempty"`
}

type OutputConfig struct {
	Format   string            `json:"format,omitempty"` // json|ndjson|csv|text
	Envelope bool              `json:"envelope,omitempty"`
	Aliases  map[string]string `json:"aliases,omitempty"`
	Omit     []string          `json:"omit,omitempty"`
	Status   int               `json:"status,omitempty"`
	Body     string            `json:"body,omitempty"`
}

type QueryRoute struct {
	Method         string            `json:"method"`
	Path           string            `json:"path"`
	QueryName      string            `json:"query_name"`
	RequireHeaders map[string]string `json:"require_headers,omitempty"`
	RateLimit      *RateLimit        `json:"rate_limit,omitempty"`
}

type RateLimit struct {
	Requests int   `json:"requests"`
	Window   int64 `json:"window_ns"`
}

type matchedRoute struct {
	method    string
	segments  []routeSegment
	queryName string
	headers   map[string]string
	rateLimit *RateLimit
	counter   map[string]*rateCounter
	counterMu sync.Mutex
}

type routeSegment struct {
	literal string
	param   string // non-empty for ":name" segments
}

type rateCounter struct {
	count   int
	resetAt time.Time
}

func buildRoute(r *QueryRoute) (*matchedRoute, error) {
	if r.Method == "" || r.Path == "" || r.QueryName == "" {
		return nil, fmt.Errorf("route requires: METHOD path query_name")
	}
	mr := &matchedRoute{
		method:    strings.ToUpper(r.Method),
		queryName: r.QueryName,
		headers:   r.RequireHeaders,
		rateLimit: r.RateLimit,
	}
	if mr.rateLimit != nil {
		mr.counter = make(map[string]*rateCounter)
	}
	for _, seg := range strings.Split(strings.Trim(r.Path, "/"), "/") {
		if strings.HasPrefix(seg, ":") {
			mr.segments = append(mr.segments, routeSegment{param: seg[1:]})
		} else {
			mr.segments = append(mr.segments, routeSegment{literal: seg})
		}
	}
	return mr, nil
}

// matchPath returns path params if the request path matches the route.
func (mr *matchedRoute) matchPath(path string) map[string]string {
	pathParts := strings.Split(strings.Trim(path, "/"), "/")
	if len(pathParts) != len(mr.segments) {
		return nil
	}
	var params map[string]string
	for i, seg := range mr.segments {
		if seg.param != "" {
			if params == nil {
				params = make(map[string]string)
			}
			params[seg.param] = pathParts[i]
			continue
		}
		if seg.literal != pathParts[i] {
			return nil
		}
	}
	if params == nil {
		params = map[string]string{}
	}
	return params
}

func (mr *matchedRoute) checkRateLimit(key string) bool {
	if mr.rateLimit == nil {
		return true
	}
	mr.counterMu.Lock()
	defer mr.counterMu.Unlock()

	now := time.Now()
	window := time.Duration(mr.rateLimit.Window)

	ctr, ok := mr.counter[key]
	if !ok || now.After(ctr.resetAt) {
		ctr = &rateCounter{count: 0, resetAt: now.Add(window)}
		mr.counter[key] = ctr
	}

	ctr.count++
	return ctr.count <= mr.rateLimit.Requests
}

// writeOutput renders rows in the configured format. cols carries the
// SELECT column order (post alias/omit) so positional formats (csv, text)
// emit columns deterministically instead of Go's random map order.
func writeOutput(w http.ResponseWriter, data []map[string]any, cols []string, out *OutputConfig) {
	if out == nil {
		out = &OutputConfig{Format: "json", Status: 200}
	}

	data, cols = transformColumns(data, cols, out)

	status := out.Status
	if status == 0 {
		status = 200
	}

	switch out.Format {
	case "csv":
		w.Header().Set("Content-Type", "text/csv")
		w.WriteHeader(status)
		writeCSV(w, data, cols)
	case "ndjson":
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(status)
		for _, row := range data {
			json.NewEncoder(w).Encode(row)
		}
	case "text":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(status)
		if out.Body != "" && len(data) == 0 {
			w.Write([]byte(out.Body))
			return
		}
		order := colOrder(cols, data)
		for _, row := range data {
			vals := make([]string, len(order))
			for i, c := range order {
				vals[i] = fmt.Sprintf("%v", row[c])
			}
			fmt.Fprintln(w, strings.Join(vals, "\t"))
		}
	default:
		if out.Body != "" && len(data) == 0 {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(status)
			w.Write([]byte(out.Body))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if out.Envelope {
			json.NewEncoder(w).Encode(map[string]any{
				"data": data,
				"meta": map[string]any{
					"count":     len(data),
					"generated": time.Now().UTC(),
				},
			})
		} else {
			json.NewEncoder(w).Encode(data)
		}
	}
}

// transformColumns applies alias/omit to rows and to the ordered column
// list so positional output formats stay in SELECT order.
func transformColumns(data []map[string]any, cols []string, out *OutputConfig) ([]map[string]any, []string) {
	if len(out.Aliases) == 0 && len(out.Omit) == 0 {
		return data, cols
	}

	omitSet := make(map[string]bool, len(out.Omit))
	for _, o := range out.Omit {
		omitSet[o] = true
	}

	result := make([]map[string]any, 0, len(data))
	for _, row := range data {
		newRow := make(map[string]any, len(row))
		for k, v := range row {
			if omitSet[k] {
				continue
			}
			if alias, ok := out.Aliases[k]; ok {
				newRow[alias] = v
			} else {
				newRow[k] = v
			}
		}
		result = append(result, newRow)
	}

	var newCols []string
	for _, c := range colOrder(cols, data) {
		if omitSet[c] {
			continue
		}
		if alias, ok := out.Aliases[c]; ok {
			newCols = append(newCols, alias)
		} else {
			newCols = append(newCols, c)
		}
	}
	return result, newCols
}

// colOrder returns the column order for positional output: the recorded
// SELECT order when available, otherwise the first row's keys sorted so
// output is at least deterministic.
func colOrder(cols []string, data []map[string]any) []string {
	if len(cols) > 0 {
		return cols
	}
	if len(data) == 0 {
		return nil
	}
	keys := make([]string, 0, len(data[0]))
	for k := range data[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func writeCSV(w http.ResponseWriter, data []map[string]any, cols []string) {
	if len(data) == 0 {
		return
	}

	cw := csv.NewWriter(w)
	defer cw.Flush()

	headers := colOrder(cols, data)
	cw.Write(headers)

	for _, row := range data {
		record := make([]string, len(headers))
		for i, h := range headers {
			record[i] = fmt.Sprintf("%v", row[h])
		}
		cw.Write(record)
	}
}

func extractIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		parts := strings.Split(fwd, ",")
		return strings.TrimSpace(parts[0])
	}
	if real := r.Header.Get("X-Real-IP"); real != "" {
		return real
	}
	idx := strings.LastIndex(r.RemoteAddr, ":")
	if idx == -1 {
		return r.RemoteAddr
	}
	return r.RemoteAddr[:idx]
}

func errorJSON(w http.ResponseWriter, status int, format string, args ...any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	msg := fmt.Sprintf(format, args...)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
