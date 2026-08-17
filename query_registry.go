package caddyturso

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2"
	"go.uber.org/zap"
)

var paramRefRe = regexp.MustCompile(`\$(\w+)`)

// QueryRegistry holds precompiled named queries.
type QueryRegistry struct {
	// db is swapped atomically when the database is restored in place.
	db      atomic.Pointer[sql.DB]
	maxRows int
	slow    time.Duration
	metrics *Metrics
	logger  *zap.Logger
	queries map[string]*registeredQuery
}

type registeredQuery struct {
	def QueryDef
	// positional SQL with "$name" rewritten to "?", computed once at Register.
	positionalSQL string
	// paramRefs lists param names in occurrence order, one entry per "?".
	paramRefs []string
	// isWrite marks statements that mutate the database; success on any
	// write invalidates all cached query results.
	isWrite bool

	cached map[string]cachedResult
	mu     sync.RWMutex
}

type cachedResult struct {
	data      []map[string]any
	cols      []string
	expiresAt time.Time
}

func NewQueryRegistry(db *sql.DB, maxRows int, slow time.Duration, metrics *Metrics, logger *zap.Logger) *QueryRegistry {
	r := &QueryRegistry{
		maxRows: maxRows,
		slow:    slow,
		metrics: metrics,
		logger:  logger,
		queries: make(map[string]*registeredQuery),
	}
	r.db.Store(db)
	return r
}

// SwapDB points the registry at a new database handle (used after an
// in-place restore) and drops all cached results.
func (r *QueryRegistry) SwapDB(db *sql.DB) {
	r.db.Store(db)
	r.InvalidateCache()
}

// paramError marks request-parameter validation failures so handlers can
// answer 400 instead of 500.
type paramError struct{ err error }

func (e paramError) Error() string { return e.err.Error() }
func (e paramError) Unwrap() error { return e.err }

func (r *QueryRegistry) Register(def *QueryDef) error {
	if def.Name == "" {
		return fmt.Errorf("missing name")
	}
	if def.SQL == "" {
		return fmt.Errorf("query %q has no sql", def.Name)
	}
	if def.Output.Format == "" {
		def.Output.Format = "json"
	}
	if def.Output.Status == 0 {
		def.Output.Status = 200
	}

	rq := &registeredQuery{def: *def, isWrite: isWriteSQL(def.SQL)}
	var refs []string
	rq.positionalSQL = paramRefRe.ReplaceAllStringFunc(def.SQL, func(match string) string {
		refs = append(refs, match[1:])
		return "?"
	})
	rq.paramRefs = refs

	// Every referenced param must have a binding so we can coerce and default.
	bound := make(map[string]bool, len(def.Params))
	for _, p := range def.Params {
		bound[p.Name] = true
	}
	for _, ref := range refs {
		if !bound[ref] {
			return fmt.Errorf("query %q references $%s without a param binding", def.Name, ref)
		}
	}

	r.queries[def.Name] = rq
	return nil
}

// ValidateAll runs EXPLAIN QUERY PLAN on every query so config errors
// surface at provision time instead of on the first request.
func (r *QueryRegistry) ValidateAll() error {
	for name, rq := range r.queries {
		args := make([]any, len(rq.paramRefs))
		rows, err := r.db.Load().Query("EXPLAIN QUERY PLAN "+rq.positionalSQL, args...)
		if err != nil {
			return fmt.Errorf("query %q: %w", name, err)
		}
		rows.Close()
	}
	return nil
}

func (r *QueryRegistry) Names() []string {
	names := make([]string, 0, len(r.queries))
	for n := range r.queries {
		names = append(names, n)
	}
	return names
}

func (r *QueryRegistry) Get(name string) (QueryDef, bool) {
	rq, ok := r.queries[name]
	if !ok {
		return QueryDef{}, false
	}
	return rq.def, true
}

// Execute resolves params from the request, runs the query, and applies
// TTL caching, slow-query logging, and metrics.
func (r *QueryRegistry) Execute(ctx context.Context, name string, req *http.Request, pathParams map[string]string) ([]map[string]any, []string, *OutputConfig, error) {
	rq, ok := r.queries[name]
	if !ok {
		return nil, nil, nil, fmt.Errorf("query %q not found", name)
	}

	values, err := resolveValues(rq.def.Params, req, pathParams)
	if err != nil {
		return nil, nil, nil, paramError{fmt.Errorf("param binding: %w", err)}
	}

	if rq.def.CacheTTL > 0 {
		if cached, cols, hit := rq.getCached(cacheKey(rq.paramRefs, values)); hit {
			r.metrics.CacheHits.Add(1)
			return cached, cols, &rq.def.Output, nil
		}
	}

	args := make([]any, len(rq.paramRefs))
	for i, ref := range rq.paramRefs {
		args[i] = values[ref]
	}

	timeout := 10 * time.Second
	if rq.def.Timeout > 0 {
		timeout = time.Duration(rq.def.Timeout)
	}
	qCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	rows, err := r.db.Load().QueryContext(qCtx, rq.positionalSQL, args...)
	elapsed := time.Since(start)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("execute %q: %w", name, err)
	}
	result, cols, err := scanRows(rows, r.maxRows)
	rows.Close()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("scan %q: %w", name, err)
	}

	r.metrics.QueriesExecuted.Add(1)
	r.metrics.RowsReturned.Add(int64(len(result)))
	r.metrics.QueryTimeMicros.Add(elapsed.Microseconds())
	if r.slow > 0 && elapsed > r.slow {
		r.metrics.SlowQueries.Add(1)
		r.logger.Warn("turso: slow query",
			zap.String("query", name),
			zap.Duration("elapsed", elapsed),
			zap.Int("rows", len(result)),
		)
	}

	// A successful write invalidates every cached read so clients never
	// observe pre-write data on their next request.
	if rq.isWrite {
		r.InvalidateCache()
	}

	if rq.def.CacheTTL > 0 {
		rq.setCached(cacheKey(rq.paramRefs, values), result, cols, time.Duration(rq.def.CacheTTL))
	}

	return result, cols, &rq.def.Output, nil
}

// writePrefixes are the SQL statement keywords that mutate database content.
var writePrefixes = []string{"INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP", "ALTER"}

// isWriteSQL reports whether the statement mutates the database.
func isWriteSQL(query string) bool {
	s := strings.ToUpper(strings.TrimSpace(query))
	for _, p := range writePrefixes {
		if strings.HasPrefix(s, p+" ") {
			return true
		}
	}
	return false
}

// InvalidateCache drops all cached query results.
func (r *QueryRegistry) InvalidateCache() {
	for _, rq := range r.queries {
		rq.mu.Lock()
		rq.cached = nil
		rq.mu.Unlock()
	}
	r.metrics.CacheInvalidations.Add(1)
}

// maxCacheEntries bounds the per-query result cache; when exceeded, the
// whole entry set is dropped instead of tracking LRU order.
const maxCacheEntries = 64

// cacheKey builds a deterministic key from the resolved param values in
// occurrence order, so identical requests share a cache entry.
func cacheKey(refs []string, values map[string]any) string {
	var b strings.Builder
	for _, ref := range refs {
		fmt.Fprintf(&b, "%v\x00", values[ref])
	}
	return b.String()
}

func (rq *registeredQuery) getCached(key string) ([]map[string]any, []string, bool) {
	rq.mu.RLock()
	defer rq.mu.RUnlock()
	if e, ok := rq.cached[key]; ok && time.Now().Before(e.expiresAt) {
		return e.data, e.cols, true
	}
	return nil, nil, false
}

func (rq *registeredQuery) setCached(key string, data []map[string]any, cols []string, ttl time.Duration) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	if rq.cached == nil {
		rq.cached = make(map[string]cachedResult)
	} else if len(rq.cached) >= maxCacheEntries {
		rq.cached = make(map[string]cachedResult)
	}
	rq.cached[key] = cachedResult{data: data, cols: cols, expiresAt: time.Now().Add(ttl)}
}

// resolveValues pulls raw values for every binding, applies defaults,
// and coerces to the declared type.
func resolveValues(bindings []ParamBinding, r *http.Request, pathParams map[string]string) (map[string]any, error) {
	values := make(map[string]any, len(bindings))

	needsBody := false
	for _, b := range bindings {
		if b.Source == "body" {
			needsBody = true
			break
		}
	}

	var bodyMap map[string]any
	if needsBody && r.Body != nil {
		bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if err == nil {
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			var body any
			if json.Unmarshal(bodyBytes, &body) == nil {
				if m, ok := body.(map[string]any); ok {
					bodyMap = m
				}
			}
		}
	}

	for _, b := range bindings {
		var raw string
		switch b.Source {
		case "query":
			raw = r.URL.Query().Get(b.Key)
		case "header":
			raw = r.Header.Get(b.Key)
		case "body":
			if bodyMap != nil {
				raw = extractBodyField(bodyMap, b.Key)
			}
		case "path":
			raw = pathParams[b.Key]
		case "env":
			raw = os.Getenv(b.Key)
		case "placeholder":
			repl, ok := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer)
			if ok {
				raw = repl.ReplaceAll("{"+b.Key+"}", "")
			}
		default:
			return nil, fmt.Errorf("param %q: unknown source %q", b.Name, b.Source)
		}

		if raw == "" && b.Default != nil {
			raw = *b.Default
		}

		coerced, err := coerce(raw, b)
		if err != nil {
			return nil, paramError{fmt.Errorf("param %q: %w", b.Name, err)}
		}
		values[b.Name] = coerced
	}

	return values, nil
}

func extractBodyField(m map[string]any, key string) string {
	parts := strings.Split(key, ".")
	var current any = m
	for _, p := range parts {
		cmap, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = cmap[p]
	}
	if current == nil {
		return ""
	}
	switch v := current.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case json.Number:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func coerce(raw string, b ParamBinding) (any, error) {
	if raw == "null" {
		return nil, nil
	}
	// An empty value means "absent". It binds as NULL (SQL's notion of
	// missing) unless the binding explicitly declares a default of "" —
	// then it stays an empty string, so predicates like
	// `WHERE ($s = '' OR s = $s)` work as intended.
	if raw == "" {
		if b.Default != nil && *b.Default == "" && (b.Type == "" || b.Type == "string") {
			return "", nil
		}
		return nil, nil
	}

	switch b.Type {
	case "int":
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("not an integer: %q", raw)
		}
		if b.Min != nil && float64(n) < *b.Min {
			return nil, fmt.Errorf("value %d below minimum %g", n, *b.Min)
		}
		if b.Max != nil && float64(n) > *b.Max {
			return nil, fmt.Errorf("value %d above maximum %g", n, *b.Max)
		}
		if b.Cap != nil && float64(n) > *b.Cap {
			n = int64(*b.Cap)
		}
		return n, nil
	case "float":
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("not a float: %q", raw)
		}
		if b.Min != nil && f < *b.Min {
			return nil, fmt.Errorf("value %g below minimum %g", f, *b.Min)
		}
		if b.Max != nil && f > *b.Max {
			return nil, fmt.Errorf("value %g above maximum %g", f, *b.Max)
		}
		if b.Cap != nil && f > *b.Cap {
			f = *b.Cap
		}
		return f, nil
	case "bool":
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("not a bool: %q", raw)
		}
		return v, nil
	default:
		if b.Pattern != "" {
			matched, err := regexp.MatchString(b.Pattern, raw)
			if err != nil {
				return nil, fmt.Errorf("invalid pattern: %w", err)
			}
			if !matched {
				return nil, fmt.Errorf("value %q doesn't match pattern %q", raw, b.Pattern)
			}
		}
		return raw, nil
	}
}

func scanRows(rows *sql.Rows, maxRows int) ([]map[string]any, []string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	result := make([]map[string]any, 0, 64)
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	for rows.Next() {
		if len(result) >= maxRows {
			break
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		result = append(result, row)
	}
	return result, cols, rows.Err()
}
