// Package caddyturso provides a Caddy HTTP handler backed by the Turso
// database engine (turso.tech/database/tursogo, purego, no CGO).
package caddyturso

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	httpcaddyfile "github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
	turso "turso.tech/database/tursogo"
)

func init() {
	caddy.RegisterModule(new(Middleware))
	httpcaddyfile.RegisterHandlerDirective("turso", parseCaddyfileHandler)
	httpcaddyfile.RegisterDirectiveOrder("turso", httpcaddyfile.Before, "file_server")
}

func parseCaddyfileHandler(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	m := new(Middleware)
	if err := m.UnmarshalCaddyfile(h.Dispenser); err != nil {
		return nil, err
	}
	return m, nil
}

// Middleware is the caddy-turso HTTP handler module.
type Middleware struct {
	// Path to the database file. Supports ":memory:" and DSN options
	// appended as a query string, e.g. "app.db?_busy_timeout=10000".
	// Caddy placeholders (e.g. {env.*}) are expanded at provision time.
	DBPath string `json:"db_path,omitempty"`

	// How long to retry opening the database during a graceful reload,
	// while the previous Caddy process still holds the file lock.
	OpenTimeout caddy.Duration `json:"open_timeout,omitempty"`

	// Max open SQL connections. Writes serialize anyway; this bounds
	// concurrent readers.
	MaxConns int `json:"max_conns,omitempty"`

	// PRAGMA statements executed once at open, e.g. "journal_mode WAL".
	// Use "name value" or full statements.
	Pragmas []string `json:"pragmas,omitempty"`

	// Schema statements executed once at provision (CREATE TABLE IF NOT EXISTS ...).
	Schema []string `json:"schema,omitempty"`

	Queries []QueryDef   `json:"queries,omitempty"`
	Routes  []QueryRoute `json:"routes,omitempty"`

	// Request logging. Disable with "log_requests off".
	LogRequests bool `json:"log_requests,omitempty"`

	ExcludePaths []string `json:"exclude_paths,omitempty"`

	// WAL checkpoint interval; 0 disables.
	Checkpoint caddy.Duration `json:"checkpoint_interval,omitempty"`

	// Online backups: periodic VACUUM INTO snapshots written to backup_dir.
	// Timestamped filenames; backup_retain prunes the oldest beyond N (0 = keep all).
	BackupDir      string         `json:"backup_dir,omitempty"`
	BackupInterval caddy.Duration `json:"backup_interval,omitempty"`
	BackupRetain   int            `json:"backup_retain,omitempty"`

	// Optional sync with a remote Turso instance (CDC-based push/pull).
	SyncURL      string         `json:"sync_url,omitempty"`
	SyncToken    string         `json:"sync_token,omitempty"`
	SyncInterval caddy.Duration `json:"sync_interval,omitempty"`
	SyncClient   string         `json:"sync_client,omitempty"`

	// Log queries slower than this as warnings; 0 disables.
	SlowQuery caddy.Duration `json:"slow_query,omitempty"`

	MaxRows int `json:"max_rows,omitempty"`

	// Management API.
	APIPath  string `json:"api_path,omitempty"`
	APIToken string `json:"api_token,omitempty"`
	RawSQL   bool   `json:"raw_sql,omitempty"`

	CORSOrigin string `json:"cors_origin,omitempty"`

	// Writer knobs for log_requests.
	BatchSize     int            `json:"batch_size,omitempty"`
	FlushInterval caddy.Duration `json:"flush_interval,omitempty"`
	BufferSize    int            `json:"buffer_size,omitempty"`
	Overflow      string         `json:"overflow,omitempty"` // drop|block

	ctx        caddy.Context
	logger     *zap.Logger
	db         atomic.Pointer[sql.DB]
	restoreMu  sync.Mutex
	syncDb     *turso.TursoSyncDb
	reg        *QueryRegistry
	writer     *BatchWriter
	routes     []*matchedRoute
	routePaths atomic.Value // []string, for debug endpoint
	metrics    Metrics
	startedAt  time.Time
}

// DB returns the current database handle. It is swapped atomically
// during a restore, so callers must use it for the whole operation.
func (m *Middleware) DB() *sql.DB {
	if p := m.db.Load(); p != nil {
		return p
	}
	return nil
}

// Metrics are runtime counters exposed at /_turso/stats.
type Metrics struct {
	QueriesExecuted    atomic.Int64
	CacheHits          atomic.Int64
	CacheInvalidations atomic.Int64
	SlowQueries        atomic.Int64
	RowsReturned       atomic.Int64
	QueryTimeMicros    atomic.Int64
	LastSyncOK         atomic.Bool
	LastSyncAt         atomic.Int64 // unix nanos
	SyncErrors         atomic.Int64
	BackupCount        atomic.Int64
	BackupErrors       atomic.Int64
	LastBackupAt       atomic.Int64 // unix nanos
	LastBackupSize     atomic.Int64
	Restores           atomic.Int64
	LastRestoreAt      atomic.Int64 // unix nanos
}

func (*Middleware) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.turso",
		New: func() caddy.Module { return new(Middleware) },
	}
}

func (m *Middleware) Provision(ctx caddy.Context) error {
	m.ctx = ctx
	m.logger = ctx.Logger(m)
	m.startedAt = time.Now()
	m.applyDefaults()

	repl := caddy.NewReplacer()
	m.DBPath = repl.ReplaceAll(m.DBPath, "")
	m.SyncURL = repl.ReplaceAll(m.SyncURL, "")
	m.SyncToken = repl.ReplaceAll(m.SyncToken, "")

	if m.DBPath == "" {
		return fmt.Errorf("turso: db_path is required (use :memory: for an in-memory database)")
	}
	if m.DBPath != ":memory:" && strings.Contains(m.DBPath, "{") {
		return fmt.Errorf("turso: db_path has unresolved placeholders after provision: %s", m.DBPath)
	}

	if err := m.openDB(ctx); err != nil {
		return err
	}

	if err := m.applyPragmas(ctx, m.DB()); err != nil {
		return err
	}

	for i, ddl := range m.Schema {
		if _, err := m.DB().Exec(ddl); err != nil {
			return fmt.Errorf("turso: schema[%d]: %w", i, err)
		}
	}

	// The _requests table must exist before query validation so that
	// registered queries can select from it.
	if m.LogRequests {
		if err := m.bootstrapRequestLog(); err != nil {
			return fmt.Errorf("turso: request log bootstrap: %w", err)
		}
		m.writer = NewBatchWriter(m.DB(), m.BatchSize, time.Duration(m.FlushInterval), m.BufferSize, m.Overflow, m.logger)
	}

	m.reg = NewQueryRegistry(m.DB(), m.MaxRows, time.Duration(m.SlowQuery), &m.metrics, m.logger)
	for i := range m.Queries {
		if err := m.reg.Register(&m.Queries[i]); err != nil {
			return fmt.Errorf("turso: register query %q: %w", m.Queries[i].Name, err)
		}
	}
	if err := m.reg.ValidateAll(); err != nil {
		return fmt.Errorf("turso: query validation: %w", err)
	}

	m.routes = make([]*matchedRoute, 0, len(m.Routes))
	var rp []string
	for i := range m.Routes {
		mr, err := buildRoute(&m.Routes[i])
		if err != nil {
			return fmt.Errorf("turso: route %s %s: %w", m.Routes[i].Method, m.Routes[i].Path, err)
		}
		m.routes = append(m.routes, mr)
		rp = append(rp, m.Routes[i].Method+" "+m.Routes[i].Path)
	}
	m.routePaths.Store(rp)

	if m.SyncURL != "" {
		go m.syncLoop(m.ctx)
	}
	if m.Checkpoint > 0 {
		go m.checkpointLoop(m.ctx)
	}
	if m.BackupDir != "" {
		go m.backupLoop(m.ctx)
	}

	m.logger.Info("turso module provisioned",
		zap.String("db_path", m.DBPath),
		zap.Int("queries", len(m.Queries)),
		zap.Int("routes", len(m.Routes)),
		zap.Bool("sync", m.SyncURL != ""),
	)
	return nil
}

func (m *Middleware) openDB(ctx context.Context) error {
	// With sync enabled, the sync engine manages the database handle.
	if m.SyncURL != "" {
		clientName := m.SyncClient
		if clientName == "" {
			clientName = "caddy-turso"
		}
		syncDb, err := turso.NewTursoSyncDb(ctx, turso.TursoSyncDbConfig{
			Path:       m.DBPath,
			RemoteUrl:  m.SyncURL,
			AuthToken:  m.SyncToken,
			ClientName: clientName,
		})
		if err != nil {
			return fmt.Errorf("turso: sync db: %w", err)
		}
		db, err := syncDb.Connect(ctx)
		if err != nil {
			return fmt.Errorf("turso: sync connect: %w", err)
		}
		m.syncDb = syncDb
		m.db.Store(db)
		return nil
	}

	db, err := m.openPlain(ctx)
	if err != nil {
		return err
	}
	m.db.Store(db)
	return nil
}

// openPlain opens the database file with a ping-retry loop: during a
// graceful reload the previous process may still hold the file lock.
func (m *Middleware) openPlain(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("turso", m.DBPath)
	if err != nil {
		return nil, fmt.Errorf("turso: open: %w", err)
	}
	if m.MaxConns > 0 {
		db.SetMaxOpenConns(m.MaxConns)
	} else {
		db.SetMaxOpenConns(4)
	}

	deadline := time.Now().Add(time.Duration(m.OpenTimeout))
	backoff := 250 * time.Millisecond
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return db, nil
		}
		if time.Now().After(deadline) {
			db.Close()
			return nil, fmt.Errorf("turso: database not reachable after %s: %w (is another caddy process holding %s?)",
				time.Duration(m.OpenTimeout), err, m.DBPath)
		}
		m.logger.Warn("turso: database busy, retrying (graceful reload overlap?)",
			zap.String("db_path", m.DBPath),
			zap.Duration("retry_in", backoff),
			zap.Error(err))
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			db.Close()
			return nil, ctx.Err()
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

func (m *Middleware) applyPragmas(ctx context.Context, db *sql.DB) error {
	for _, p := range m.Pragmas {
		if _, err := db.ExecContext(ctx, p); err != nil {
			return fmt.Errorf("turso: pragma %q: %w", p, err)
		}
	}
	return nil
}

func (m *Middleware) bootstrapRequestLog() error {
	_, err := m.DB().Exec(`CREATE TABLE IF NOT EXISTS _requests (
		id         INTEGER PRIMARY KEY,
		ts         TEXT    NOT NULL,
		ip         TEXT    NOT NULL,
		method     TEXT    NOT NULL,
		host       TEXT,
		path       TEXT,
		query      TEXT,
		status     INTEGER NOT NULL,
		latency_ms INTEGER NOT NULL,
		bytes_sent INTEGER NOT NULL,
		user_agent TEXT
	)`)
	if err != nil {
		return err
	}
	_, err = m.DB().Exec(`CREATE INDEX IF NOT EXISTS idx_requests_ts ON _requests (ts)`)
	return err
}

func (m *Middleware) Cleanup() error {
	if m.writer != nil {
		m.writer.Stop()
	}
	if db := m.DB(); db != nil {
		if m.Checkpoint > 0 || m.syncDb != nil {
			m.checkpointOnce()
		}
		return db.Close()
	}
	return nil
}

func (m *Middleware) Validate() error {
	names := make(map[string]bool, len(m.Queries))
	for _, q := range m.Queries {
		if q.Name == "" {
			return fmt.Errorf("turso: query missing name")
		}
		names[q.Name] = true
	}
	for _, r := range m.Routes {
		if !names[r.QueryName] {
			return fmt.Errorf("turso: route %s %s references unknown query %q", r.Method, r.Path, r.QueryName)
		}
	}
	if m.SyncURL != "" && m.SyncToken == "" {
		return fmt.Errorf("turso: sync_token is required when sync_url is set")
	}
	if m.Overflow != "" && m.Overflow != "drop" && m.Overflow != "block" {
		return fmt.Errorf("turso: overflow must be drop or block, got %q", m.Overflow)
	}
	return nil
}

func (m *Middleware) applyDefaults() {
	if m.OpenTimeout == 0 {
		m.OpenTimeout = caddy.Duration(15 * time.Second)
	}
	if m.MaxRows == 0 {
		m.MaxRows = 10000
	}
	if m.APIPath == "" {
		m.APIPath = "/_turso"
	}
	if m.SlowQuery == 0 {
		m.SlowQuery = caddy.Duration(200 * time.Millisecond)
	}
	if m.SyncInterval == 0 {
		m.SyncInterval = caddy.Duration(30 * time.Second)
	}
	if m.BatchSize == 0 {
		m.BatchSize = 500
	}
	if m.FlushInterval == 0 {
		m.FlushInterval = caddy.Duration(200 * time.Millisecond)
	}
	if m.BufferSize == 0 {
		m.BufferSize = 8192
	}
	if m.Overflow == "" {
		m.Overflow = "drop"
	}
	if m.BackupDir != "" && m.BackupInterval == 0 {
		m.BackupInterval = caddy.Duration(6 * time.Hour)
	}
}

// backupOnce writes a consistent snapshot of the database to backup_dir
// using VACUUM INTO (works while the database is serving traffic).
func (m *Middleware) backupOnce(ctx context.Context) (string, int64, error) {
	if err := os.MkdirAll(m.BackupDir, 0o755); err != nil {
		return "", 0, err
	}
	path, err := m.nextBackupPath("")
	if err != nil {
		return "", 0, err
	}
	size, err := m.vacuumInto(ctx, path)
	if err != nil {
		return "", 0, err
	}
	m.pruneBackups(m.dbBaseName())
	return path, size, nil
}

// nextBackupPath returns a not-yet-existing snapshot path of the form
// <base>-<timestamp><infix>.db inside backup_dir.
func (m *Middleware) nextBackupPath(infix string) (string, error) {
	base := m.dbBaseName()
	stamp := time.Now().UTC().Format("20060102-150405")
	path := filepath.Join(m.BackupDir, base+"-"+stamp+infix+".db")
	for i := 2; ; i++ {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return path, nil
		} else if err != nil {
			return "", err
		}
		path = filepath.Join(m.BackupDir, fmt.Sprintf("%s-%s%s-%d.db", base, stamp, infix, i))
	}
}

// vacuumInto writes a consistent snapshot to path. The engine requires
// the target as a string literal, not a bound parameter.
func (m *Middleware) vacuumInto(ctx context.Context, path string) (int64, error) {
	literal := strings.ReplaceAll(path, "'", "''")
	if _, err := m.DB().ExecContext(ctx, "VACUUM INTO '"+literal+"'"); err != nil {
		return 0, err
	}
	if st, err := os.Stat(path); err == nil {
		return st.Size(), nil
	}
	return 0, nil
}

// backupEntry describes one snapshot file in backup_dir.
type backupEntry struct {
	Name     string    `json:"name"`
	Bytes    int64     `json:"bytes"`
	Modified time.Time `json:"modified"`
}

// listBackups returns metadata for every snapshot in backup_dir,
// newest first.
func (m *Middleware) listBackups() ([]backupEntry, error) {
	entries, err := os.ReadDir(m.BackupDir)
	if err != nil {
		return nil, err
	}
	prefix := m.dbBaseName() + "-"
	var files []backupEntry
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), prefix) || !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, backupEntry{
			Name:     e.Name(),
			Bytes:    info.Size(),
			Modified: info.ModTime().UTC(),
		})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Modified.After(files[j].Modified) })
	return files, nil
}

// dbFilePath returns db_path without DSN query parameters.
func (m *Middleware) dbFilePath() string {
	if i := strings.IndexByte(m.DBPath, '?'); i >= 0 {
		return m.DBPath[:i]
	}
	return m.DBPath
}

// restoreBackup swaps the database file for a snapshot from backup_dir
// and reopens the database in place. A safety snapshot of the current
// state is taken first, so a restore can itself be undone. Queries that
// are in flight while the old handle closes may fail; the next request
// uses the restored database.
func (m *Middleware) restoreBackup(ctx context.Context, name string) (map[string]any, error) {
	if name != filepath.Base(name) || strings.ContainsAny(name, `/\`) || strings.Contains(name, "..") {
		return nil, fmt.Errorf("invalid backup name %q", name)
	}
	base := m.dbBaseName()
	if !strings.HasPrefix(name, base+"-") || !strings.HasSuffix(name, ".db") {
		return nil, fmt.Errorf("backup name %q does not match %s-*.db in %s", name, base, m.BackupDir)
	}
	src := filepath.Join(m.BackupDir, name)
	st, err := os.Stat(src)
	if err != nil {
		return nil, err
	}

	// Safety snapshot of the current state (best effort: a corrupted
	// database must still be restorable).
	safety := ""
	if safetyPath, err := m.nextBackupPath("-pre-restore"); err == nil {
		if _, err := m.vacuumInto(ctx, safetyPath); err == nil {
			safety = filepath.Base(safetyPath)
		} else {
			m.logger.Warn("turso: pre-restore safety snapshot failed", zap.Error(err))
		}
	}

	// Drain the request log, checkpoint, then close the old handle.
	if m.writer != nil {
		m.writer.Flush()
	}
	if db := m.DB(); db != nil {
		_, _ = db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
		db.Close()
	}

	// Swap the file via a temp copy so a crash never half-writes the db.
	dest := m.dbFilePath()
	tmp := dest + ".restore-tmp"
	if err := copyFile(src, tmp); err != nil {
		return nil, fmt.Errorf("copy snapshot: %w", err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		os.Remove(tmp)
		return nil, fmt.Errorf("swap database file: %w", err)
	}
	// Sidecars from the previous database must not leak into the new one.
	os.Remove(dest + "-wal")
	os.Remove(dest + "-shm")

	db, err := m.openPlain(ctx)
	if err != nil {
		return nil, fmt.Errorf("reopen after restore: %w (restart caddy if this persists)", err)
	}
	if err := m.applyPragmas(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	m.db.Store(db)
	m.reg.SwapDB(db) // also invalidates cached results
	if m.writer != nil {
		m.writer.SetDB(db)
	}
	m.metrics.Restores.Add(1)
	m.metrics.LastRestoreAt.Store(time.Now().UnixNano())
	m.logger.Info("turso: database restored",
		zap.String("from", src),
		zap.String("safety_backup", safety))
	return map[string]any{
		"restored":      name,
		"bytes":         st.Size(),
		"safety_backup": safety,
	}, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func (m *Middleware) dbBaseName() string {
	name := m.DBPath
	if i := strings.IndexByte(name, '?'); i >= 0 {
		name = name[:i]
	}
	name = filepath.Base(name)
	if name == "" || name == "." || name == string(filepath.Separator) {
		name = "turso"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			return r
		default:
			return '_'
		}
	}, name)
}

// pruneBackups keeps only the newest backup_retain snapshots (0 = keep all).
func (m *Middleware) pruneBackups(base string) {
	if m.BackupRetain <= 0 {
		return
	}
	entries, err := os.ReadDir(m.BackupDir)
	if err != nil {
		return
	}
	prefix := base + "-"
	type backupFile struct {
		name    string
		modTime time.Time
	}
	var files []backupFile
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), prefix) && strings.HasSuffix(e.Name(), ".db") {
			info, err := e.Info()
			if err != nil {
				continue
			}
			files = append(files, backupFile{e.Name(), info.ModTime()})
		}
	}
	if len(files) <= m.BackupRetain {
		return
	}
	sort.Slice(files, func(i, j int) bool { return files[i].modTime.Before(files[j].modTime) })
	for _, f := range files[:len(files)-m.BackupRetain] {
		if err := os.Remove(filepath.Join(m.BackupDir, f.name)); err == nil {
			m.logger.Info("turso: pruned old backup", zap.String("file", f.name))
		}
	}
}

func (m *Middleware) backupLoop(ctx caddy.Context) {
	ticker := time.NewTicker(time.Duration(m.BackupInterval))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.runBackup(context.Background())
		case <-ctx.Done():
			return
		}
	}
}

func (m *Middleware) runBackup(ctx context.Context) {
	bctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	path, size, err := m.backupOnce(bctx)
	if err != nil {
		m.metrics.BackupErrors.Add(1)
		m.logger.Error("turso: backup failed", zap.Error(err))
		return
	}
	m.metrics.BackupCount.Add(1)
	m.metrics.LastBackupAt.Store(time.Now().UnixNano())
	m.metrics.LastBackupSize.Store(size)
	m.logger.Info("turso: backup complete", zap.String("file", path), zap.Int64("bytes", size))
}

func (m *Middleware) syncCycle(ctx context.Context) error {
	if err := m.syncDb.Push(ctx); err != nil {
		return fmt.Errorf("push: %w", err)
	}
	if _, err := m.syncDb.Pull(ctx); err != nil {
		return fmt.Errorf("pull: %w", err)
	}
	return nil
}

func (m *Middleware) syncLoop(ctx caddy.Context) {
	ticker := time.NewTicker(time.Duration(m.SyncInterval))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			syncCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := m.syncCycle(syncCtx); err != nil {
				m.metrics.SyncErrors.Add(1)
				m.metrics.LastSyncOK.Store(false)
				m.logger.Error("turso: sync cycle failed", zap.Error(err))
			} else {
				m.metrics.LastSyncOK.Store(true)
				m.metrics.LastSyncAt.Store(time.Now().UnixNano())
			}
			cancel()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Middleware) checkpointOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var err error
	if m.syncDb != nil {
		err = m.syncDb.Checkpoint(ctx)
	} else if db := m.DB(); db != nil {
		_, err = db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	}
	if err != nil {
		m.logger.Warn("turso: checkpoint failed", zap.Error(err))
	}
}

func (m *Middleware) checkpointLoop(ctx caddy.Context) {
	ticker := time.NewTicker(time.Duration(m.Checkpoint))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.checkpointOnce()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Middleware) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next()
	for d.NextBlock(0) {
		switch d.Val() {
		case "db_path":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.DBPath = d.Val()
		case "open_timeout":
			dur, err := parseDurationArg(d)
			if err != nil {
				return err
			}
			m.OpenTimeout = caddy.Duration(dur)
		case "max_conns":
			n, err := parseIntArg(d)
			if err != nil {
				return err
			}
			m.MaxConns = n
		case "pragma":
			if !d.NextArg() {
				return d.ArgErr()
			}
			first := d.Val()
			rest := d.RemainingArgs()
			var stmt string
			if strings.HasPrefix(strings.ToUpper(first), "PRAGMA") {
				stmt = first // already a full statement
			} else if len(rest) == 1 {
				// the engine requires the "name = value" form
				stmt = "PRAGMA " + first + " = " + rest[0]
			} else if len(rest) > 1 {
				stmt = "PRAGMA " + first + " " + strings.Join(rest, " ")
			} else {
				stmt = "PRAGMA " + first
			}
			m.Pragmas = append(m.Pragmas, stmt)
		case "schema":
			if !d.NextArg() {
				return d.ArgErr()
			}
			raw := d.Val()
			if strings.HasPrefix(raw, "file:") {
				data, err := os.ReadFile(strings.TrimPrefix(raw, "file:"))
				if err != nil {
					return d.Errf("turso: read schema file: %v", err)
				}
				raw = string(data)
			}
			m.Schema = append(m.Schema, raw)
		case "query":
			qd, err := parseQueryDef(d)
			if err != nil {
				return err
			}
			m.Queries = append(m.Queries, qd)
		case "route":
			r, err := parseQueryRoute(d)
			if err != nil {
				return err
			}
			m.Routes = append(m.Routes, r)
		case "log_requests":
			if d.NextArg() {
				m.LogRequests = d.Val() == "on" || d.Val() == "true"
			} else {
				m.LogRequests = true
			}
		case "exclude_path":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.ExcludePaths = append(m.ExcludePaths, d.Val())
		case "checkpoint":
			dur, err := parseDurationArg(d)
			if err != nil {
				return err
			}
			m.Checkpoint = caddy.Duration(dur)
		case "backup_dir":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.BackupDir = d.Val()
		case "backup_interval":
			dur, err := parseDurationArg(d)
			if err != nil {
				return err
			}
			m.BackupInterval = caddy.Duration(dur)
		case "backup_retain":
			n, err := parseIntArg(d)
			if err != nil {
				return err
			}
			m.BackupRetain = n
		case "sync_url":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.SyncURL = d.Val()
		case "sync_token":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.SyncToken = d.Val()
		case "sync_interval":
			dur, err := parseDurationArg(d)
			if err != nil {
				return err
			}
			m.SyncInterval = caddy.Duration(dur)
		case "sync_client":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.SyncClient = d.Val()
		case "slow_query":
			dur, err := parseDurationArg(d)
			if err != nil {
				return err
			}
			m.SlowQuery = caddy.Duration(dur)
		case "max_rows":
			n, err := parseIntArg(d)
			if err != nil {
				return err
			}
			m.MaxRows = n
		case "batch_size":
			n, err := parseIntArg(d)
			if err != nil {
				return err
			}
			m.BatchSize = n
		case "flush_interval":
			dur, err := parseDurationArg(d)
			if err != nil {
				return err
			}
			m.FlushInterval = caddy.Duration(dur)
		case "buffer_size":
			n, err := parseIntArg(d)
			if err != nil {
				return err
			}
			m.BufferSize = n
		case "overflow":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.Overflow = d.Val()
		case "api_path":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.APIPath = d.Val()
		case "api_token":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.APIToken = d.Val()
		case "raw_sql":
			if d.NextArg() {
				m.RawSQL = d.Val() == "on" || d.Val() == "true"
			} else {
				m.RawSQL = true
			}
		case "cors_origin":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.CORSOrigin = d.Val()
		default:
			return d.Errf("turso: unknown directive: %s", d.Val())
		}
	}
	if m.DBPath == "" {
		return d.Err("turso: db_path is required")
	}
	return m.Validate()
}

func parseDurationArg(d *caddyfile.Dispenser) (time.Duration, error) {
	if !d.NextArg() {
		return 0, d.ArgErr()
	}
	dur, err := caddy.ParseDuration(d.Val())
	if err != nil {
		return 0, d.Errf("turso: invalid duration %q: %v", d.Val(), err)
	}
	return dur, nil
}

func parseIntArg(d *caddyfile.Dispenser) (int, error) {
	if !d.NextArg() {
		return 0, d.ArgErr()
	}
	n, err := strconv.Atoi(d.Val())
	if err != nil {
		return 0, d.Errf("turso: invalid integer %q: %v", d.Val(), err)
	}
	return n, nil
}

func expandHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return path
		}
		return filepath.Join(home, path[2:])
	}
	if path == "~" {
		home, err := os.UserHomeDir()
		if err != nil {
			return path
		}
		return home
	}
	return path
}

var (
	_ caddy.Provisioner           = (*Middleware)(nil)
	_ caddy.CleanerUpper          = (*Middleware)(nil)
	_ caddy.Validator             = (*Middleware)(nil)
	_ caddyfile.Unmarshaler       = (*Middleware)(nil)
	_ caddyhttp.MiddlewareHandler = (*Middleware)(nil)
)
