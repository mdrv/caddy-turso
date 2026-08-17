package caddyturso

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
)

func (m *Middleware) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	if m.CORSOrigin != "" {
		w.Header().Set("Access-Control-Allow-Origin", m.CORSOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if m.CORSOrigin != "*" {
			w.Header().Set("Vary", "Origin")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return nil
		}
	}

	shouldLog := m.writer != nil && !m.isExcludedPath(r.URL.Path)
	if !shouldLog {
		return m.serveRoutes(w, r, next)
	}

	start := time.Now()
	rec := caddyhttp.NewResponseRecorder(w, nil, nil)
	err := m.serveRoutes(rec, r, next)
	if err != nil {
		return err
	}

	m.writer.Write(RequestRecord{
		TS:        time.Now().UTC().Format(time.RFC3339Nano),
		IP:        extractIP(r),
		Method:    r.Method,
		Host:      r.Host,
		Path:      r.URL.Path,
		Query:     r.URL.RawQuery,
		Status:    rec.Status(),
		LatencyMs: time.Since(start).Milliseconds(),
		BytesSent: int64(rec.Size()),
		UserAgent: r.UserAgent(),
	})
	return nil
}

// serveRoutes handles route-matched queries, the management API, and
// finally falls through to the next handler.
func (m *Middleware) serveRoutes(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	for i := range m.routes {
		mr := m.routes[i]
		if mr.method != "*" && mr.method != r.Method {
			continue
		}
		params := mr.matchPath(r.URL.Path)
		if params == nil {
			continue
		}

		for hdr, expected := range mr.headers {
			if r.Header.Get(hdr) != expected {
				errorJSON(w, http.StatusUnauthorized, "unauthorized")
				return nil
			}
		}

		if mr.rateLimit != nil {
			if !mr.checkRateLimit(extractIP(r)) {
				errorJSON(w, http.StatusTooManyRequests, "rate limited")
				return nil
			}
		}

		data, cols, out, err := m.reg.Execute(r.Context(), mr.queryName, r, params)
		if err != nil {
			m.logger.Error("turso: query execution failed",
				zap.String("query", mr.queryName),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Error(err),
			)
			status := http.StatusInternalServerError
			var pe paramError
			if errors.As(err, &pe) {
				status = http.StatusBadRequest
			}
			errorJSON(w, status, "%v", err)
			return nil
		}
		writeOutput(w, data, cols, out)
		return nil
	}

	if r.URL.Path == m.APIPath || strings.HasPrefix(r.URL.Path, m.APIPath+"/") {
		m.serveAPI(w, r)
		return nil
	}

	return next.ServeHTTP(w, r)
}

func (m *Middleware) isExcludedPath(path string) bool {
	for _, p := range m.ExcludePaths {
		if strings.HasPrefix(p, "*") && strings.HasSuffix(path, p[1:]) {
			return true
		}
		if strings.HasSuffix(p, "*") && strings.HasPrefix(path, p[:len(p)-1]) {
			return true
		}
		if path == p {
			return true
		}
	}
	return false
}

// serveAPI exposes the management/debug API:
//
//	GET  /_turso/health      ping the database
//	GET  /_turso/stats       runtime metrics (+ sync stats when enabled)
//	GET  /_turso/queries     list registered queries
//	GET  /_turso/query/<name>
//	POST /_turso/sql         raw SQL (requires raw_sql on)
//	POST /_turso/sync        trigger a push+pull cycle (sync only)
//	POST /_turso/backup      write a snapshot now (backup_dir only)
//	GET  /_turso/backups     list snapshots (backup_dir only)
//	POST /_turso/restore     swap in a snapshot (backup_dir only)
func (m *Middleware) serveAPI(w http.ResponseWriter, r *http.Request) {
	if m.APIToken != "" {
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if tok != m.APIToken {
			errorJSON(w, http.StatusUnauthorized, "unauthorized")
			return
		}
	}

	path := strings.TrimPrefix(r.URL.Path, m.APIPath)
	path = strings.TrimPrefix(path, "/")

	switch {
	case path == "health":
		if r.Method != http.MethodGet {
			errorJSON(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		if err := m.DB().PingContext(r.Context()); err != nil {
			errorJSON(w, http.StatusServiceUnavailable, "database unreachable: %v", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})

	case path == "stats":
		if r.Method != http.MethodGet {
			errorJSON(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		m.serveStats(w, r)

	case path == "" || path == "queries":
		if r.Method != http.MethodGet {
			errorJSON(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"queries": m.reg.Names()})

	case strings.HasPrefix(path, "query/"):
		if r.Method != http.MethodGet {
			errorJSON(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		name := strings.TrimPrefix(path, "query/")
		if _, ok := m.reg.Get(name); !ok {
			errorJSON(w, http.StatusNotFound, "no such query: %s", name)
			return
		}
		data, cols, out, err := m.reg.Execute(r.Context(), name, r, nil)
		if err != nil {
			errorJSON(w, http.StatusBadRequest, "%v", err)
			return
		}
		writeOutput(w, data, cols, out)
	case path == "sql":
		if !m.RawSQL {
			errorJSON(w, http.StatusForbidden, "raw SQL disabled (enable with raw_sql)")
			return
		}
		if r.Method != http.MethodPost {
			errorJSON(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		m.serveRawSQL(w, r)

	case path == "sync":
		if m.syncDb == nil {
			errorJSON(w, http.StatusPreconditionFailed, "sync not configured")
			return
		}
		if r.Method != http.MethodPost {
			errorJSON(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		if err := m.syncCycle(r.Context()); err != nil {
			m.metrics.SyncErrors.Add(1)
			m.metrics.LastSyncOK.Store(false)
			errorJSON(w, http.StatusInternalServerError, "sync failed: %v", err)
			return
		}
		m.metrics.LastSyncOK.Store(true)
		m.metrics.LastSyncAt.Store(time.Now().UnixNano())
		writeJSON(w, http.StatusOK, map[string]any{"status": "synced"})

	case path == "backup":
		if m.BackupDir == "" {
			errorJSON(w, http.StatusPreconditionFailed, "backup_dir not configured")
			return
		}
		if r.Method != http.MethodPost {
			errorJSON(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		bctx, cancel := contextWithTimeout(r.Context(), 5*time.Minute)
		defer cancel()
		path, size, err := m.backupOnce(bctx)
		if err != nil {
			m.metrics.BackupErrors.Add(1)
			errorJSON(w, http.StatusInternalServerError, "backup failed: %v", err)
			return
		}
		m.metrics.BackupCount.Add(1)
		m.metrics.LastBackupAt.Store(time.Now().UnixNano())
		m.metrics.LastBackupSize.Store(size)
		writeJSON(w, http.StatusOK, map[string]any{"backup": path, "bytes": size})

	case path == "backups":
		if m.BackupDir == "" {
			errorJSON(w, http.StatusPreconditionFailed, "backup_dir not configured")
			return
		}
		if r.Method != http.MethodGet {
			errorJSON(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		backups, err := m.listBackups()
		if err != nil {
			errorJSON(w, http.StatusInternalServerError, "list backups: %v", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"backups": backups, "count": len(backups)})

	case path == "restore":
		if m.BackupDir == "" {
			errorJSON(w, http.StatusPreconditionFailed, "backup_dir not configured")
			return
		}
		if m.syncDb != nil {
			errorJSON(w, http.StatusPreconditionFailed, "restore is not supported while sync is enabled")
			return
		}
		if r.Method != http.MethodPost {
			errorJSON(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		name := r.URL.Query().Get("name")
		if name == "" {
			var body struct {
				Name string `json:"name"`
			}
			if strings.Contains(r.Header.Get("Content-Type"), "application/json") {
				if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
					errorJSON(w, http.StatusBadRequest, "invalid JSON: %v", err)
					return
				}
			}
			name = body.Name
		}
		if name == "" {
			errorJSON(w, http.StatusBadRequest, `missing backup name (JSON body {"name": ...} or ?name=)`)
			return
		}
		if name != filepath.Base(name) || strings.Contains(name, "..") {
			errorJSON(w, http.StatusBadRequest, "invalid backup name %q", name)
			return
		}
		if !strings.HasPrefix(name, m.dbBaseName()+"-") || !strings.HasSuffix(name, ".db") {
			errorJSON(w, http.StatusBadRequest, "backup name %q is not a %s-*.db snapshot in %s", name, m.dbBaseName(), m.BackupDir)
			return
		}
		if strings.HasPrefix(m.DBPath, ":memory:") {
			errorJSON(w, http.StatusBadRequest, "restore requires a file-backed db_path")
			return
		}
		if !m.restoreMu.TryLock() {
			errorJSON(w, http.StatusConflict, "restore already in progress")
			return
		}
		defer m.restoreMu.Unlock()
		res, err := m.restoreBackup(r.Context(), name)
		if err != nil {
			if os.IsNotExist(err) {
				errorJSON(w, http.StatusNotFound, "no such backup: %v", err)
				return
			}
			errorJSON(w, http.StatusInternalServerError, "restore failed: %v", err)
			return
		}
		writeJSON(w, http.StatusOK, res)

	default:
		errorJSON(w, http.StatusNotFound, "unknown api path: %s", path)
	}
}

func (m *Middleware) serveStats(w http.ResponseWriter, r *http.Request) {
	stats := map[string]any{
		"uptime_s":            int(time.Since(m.startedAt).Seconds()),
		"queries":             m.metrics.QueriesExecuted.Load(),
		"cache_hits":          m.metrics.CacheHits.Load(),
		"cache_invalidations": m.metrics.CacheInvalidations.Load(),
		"rows":                m.metrics.RowsReturned.Load(),
		"slow_queries":        m.metrics.SlowQueries.Load(),
		"query_time_us":       m.metrics.QueryTimeMicros.Load(),
	}
	if n := m.metrics.LastSyncAt.Load(); n > 0 {
		stats["last_sync"] = time.Unix(0, n).UTC()
		stats["last_sync_ok"] = m.metrics.LastSyncOK.Load()
	}
	stats["sync_errors"] = m.metrics.SyncErrors.Load()
	if n := m.metrics.LastBackupAt.Load(); n > 0 {
		stats["last_backup"] = time.Unix(0, n).UTC()
		stats["last_backup_bytes"] = m.metrics.LastBackupSize.Load()
	}
	stats["backup_count"] = m.metrics.BackupCount.Load()
	stats["backup_errors"] = m.metrics.BackupErrors.Load()
	stats["restores"] = m.metrics.Restores.Load()
	if n := m.metrics.LastRestoreAt.Load(); n > 0 {
		stats["last_restore"] = time.Unix(0, n).UTC()
	}

	if m.writer != nil {
		stats["log_buffer"] = map[string]any{
			"depth":   m.writer.Buffered(),
			"dropped": m.writer.Dropped(),
		}
	}
	if m.syncDb != nil {
		syncStats, err := m.syncDb.Stats(r.Context())
		if err != nil {
			stats["sync_stats_error"] = err.Error()
		} else {
			stats["sync"] = syncStats
		}
	}
	writeJSON(w, http.StatusOK, stats)
}

func (m *Middleware) serveRawSQL(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SQL    string `json:"sql"`
		Params []any  `json:"params,omitempty"`
	}
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, "application/json") {
		if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&body); err != nil {
			errorJSON(w, http.StatusBadRequest, "invalid JSON: %v", err)
			return
		}
	} else {
		buf, _ := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		body.SQL = strings.TrimSpace(string(buf))
	}
	if body.SQL == "" {
		errorJSON(w, http.StatusBadRequest, "empty sql")
		return
	}

	ctx, cancel := contextWithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	rows, err := m.DB().QueryContext(ctx, body.SQL, body.Params...)
	if err != nil {
		errorJSON(w, http.StatusBadRequest, "%v", err)
		return
	}
	defer rows.Close()

	data, cols, err := scanRows(rows, m.MaxRows)
	if err != nil {
		errorJSON(w, http.StatusInternalServerError, "%v", err)
		return
	}
	if isWriteSQL(body.SQL) {
		m.reg.InvalidateCache()
	}

	writeOutput(w, data, cols, &OutputConfig{Format: "json", Envelope: true, Status: 200})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(v)
}

func contextWithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) < d {
		return context.WithDeadline(parent, deadline)
	}
	return context.WithTimeout(parent, d)
}
