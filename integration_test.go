package caddyturso_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddytest"
)

func setup(t *testing.T) (*caddytest.Tester, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	tc := caddytest.NewTester(t)
	tc.InitServer(caddyfile(dbPath), "caddyfile")
	return tc, dbPath
}

func caddyfile(dbPath string) string {
	return fmt.Sprintf(`
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	route {
		turso {
			db_path %q

			schema `+"`"+`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL)`+"`"+`

			query list_users {
				sql "SELECT id, name, email FROM users ORDER BY id"
				cache 1ms
				output {
					format json
				}
			}

			query get_user {
				sql "SELECT id, name, email FROM users WHERE id = $id"
				param $id {
					from path
					key id
					type int
				}
				output {
					format text
				}
			}

			query insert_user {
				sql "INSERT INTO users (name, email) VALUES ($name, $email) RETURNING id, name, email"
				param $name {
					from body
					key name
				}
				param $email {
					from body
					key email
				}
				output {
					status 201
					envelope
				}
			}

			route GET /api/users list_users
			route GET /api/users/:id get_user
			route POST /api/users insert_user
		}
		respond "fallback" 404
	}
}
`, dbPath)
}

func do(t *testing.T, tc *caddytest.Tester, method, uri, body string, headers ...string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, uri, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := tc.Client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(raw)
}

func TestInsertAndQuery(t *testing.T) {
	tc, _ := setup(t)

	code, body := do(t, tc, "POST", "http://localhost:9080/api/users",
		`{"name":"Ada","email":"ada@example.com"}`, "Content-Type", "application/json")
	if code != http.StatusCreated {
		t.Fatalf("expected 201, got %d; body: %s", code, body)
	}
	if !strings.Contains(body, `"name":"Ada"`) {
		t.Fatalf("expected Ada in body, got: %s", body)
	}

	code, body = do(t, tc, "GET", "http://localhost:9080/api/users", "")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", code, body)
	}
	if !strings.Contains(body, "ada@example.com") {
		t.Fatalf("expected ada@example.com in body, got: %s", body)
	}

	code, body = do(t, tc, "GET", "http://localhost:9080/api/users/1", "")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", code, body)
	}
	if !strings.Contains(body, "Ada") {
		t.Fatalf("expected Ada in body, got: %s", body)
	}
}

func TestFallback(t *testing.T) {
	tc, _ := setup(t)
	code, body := do(t, tc, "GET", "http://localhost:9080/nope", "")
	if code != http.StatusNotFound || body != "fallback" {
		t.Fatalf("expected 404 fallback, got %d %q", code, body)
	}
}

func TestHealthAndStats(t *testing.T) {
	tc, _ := setup(t)

	code, body := do(t, tc, "GET", "http://localhost:9080/_turso/health", "")
	if code != http.StatusOK || !strings.Contains(body, `"status":"ok"`) {
		t.Fatalf("health: got %d %s", code, body)
	}

	do(t, tc, "GET", "http://localhost:9080/api/users", "")

	code, body = do(t, tc, "GET", "http://localhost:9080/_turso/stats", "")
	if code != http.StatusOK {
		t.Fatalf("stats: got %d %s", code, body)
	}
	if !strings.Contains(body, `"queries":1`) {
		t.Fatalf("expected queries:1 in stats, got: %s", body)
	}
}

func TestListQueries(t *testing.T) {
	tc, _ := setup(t)
	code, body := do(t, tc, "GET", "http://localhost:9080/_turso/queries", "")
	if code != http.StatusOK {
		t.Fatalf("queries: got %d %s", code, body)
	}
	for _, q := range []string{"list_users", "get_user", "insert_user"} {
		if !strings.Contains(body, q) {
			t.Fatalf("expected %s in %s", q, body)
		}
	}
}

func TestPragmas(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pragma.db")
	tc := caddytest.NewTester(t)
	tc.InitServer(fmt.Sprintf(`
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	route {
		turso {
			db_path %q
			pragma journal_mode WAL
			pragma "PRAGMA synchronous = NORMAL"
			raw_sql on
			schema `+"`"+`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)`+"`"+`
		}
	}
}
`, dbPath), "caddyfile")

	// Regression: the pragma parser used to drop the pragma name, turning
	// "pragma journal_mode WAL" into Exec("WAL"), which fails at provision.
	code, body := do(t, tc, "POST", "http://localhost:9080/_turso/sql",
		`{"sql":"PRAGMA journal_mode"}`, "Content-Type", "application/json")
	if code != http.StatusOK {
		t.Fatalf("raw sql: got %d %s", code, body)
	}
	if !strings.Contains(body, "wal") {
		t.Fatalf("expected journal_mode wal, got: %s", body)
	}
}

func TestCacheKeyedByParams(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "cache.db")
	tc := caddytest.NewTester(t)
	tc.InitServer(fmt.Sprintf(`
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	route {
		turso {
			db_path %q
			schema `+"`"+`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)`+"`"+`
			query get_user {
				sql "SELECT id, name FROM users WHERE id = $id"
				param $id {
					from query
					key id
					type int
				}
				cache 10s
			}
			query add_user {
				sql "INSERT INTO users (id, name) VALUES ($id, $name)"
				param $id {
					from body
					key id
					type int
				}
				param $name {
					from body
					key name
				}
			}
			route POST /users add_user
		}
		respond "fallback" 404
	}
}
`, dbPath), "caddyfile")

	do(t, tc, "POST", "http://localhost:9080/users", `{"id":1,"name":"Ada"}`, "Content-Type", "application/json")
	do(t, tc, "POST", "http://localhost:9080/users", `{"id":2,"name":"Bob"}`, "Content-Type", "application/json")

	// Two different params inside the cache TTL must not share an entry
	// (regression: the cache used to be a single slot per query).
	code, body := do(t, tc, "GET", "http://localhost:9080/_turso/query/get_user?id=1", "")
	if code != http.StatusOK || !strings.Contains(body, "Ada") {
		t.Fatalf("id=1: got %d %s", code, body)
	}
	code, body = do(t, tc, "GET", "http://localhost:9080/_turso/query/get_user?id=2", "")
	if code != http.StatusOK || !strings.Contains(body, "Bob") {
		t.Fatalf("id=2: got %d %s (expected Bob, not a stale Ada entry)", code, body)
	}
}

// loadFails asserts that a broken config is rejected by the running admin
// API with an error mentioning want. (caddytest.AssertLoadError panics in
// v2.11.4, so we drive the /load endpoint ourselves.)
func loadFails(t *testing.T, badConfig, want string) {
	t.Helper()
	tc := caddytest.NewTester(t)
	tc.InitServer(`
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	respond "ok" 200
}
`, "caddyfile")

	req, err := http.NewRequest("POST", "http://localhost:2999/load", strings.NewReader(badConfig))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "text/caddyfile")
	resp, err := tc.Client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if !strings.Contains(string(body), want) {
		t.Fatalf("expected error to mention %q, got: %s", want, body)
	}
	if !strings.Contains(string(body), `"error"`) {
		t.Fatalf("expected an error object in response, got: %s", body)
	}
}

func TestBadQueryFailsAtProvision(t *testing.T) {
	loadFails(t, `
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	turso {
		db_path ":memory:"
		schema `+"`"+`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY)`+"`"+`
		query broken {
			sql "SELECT nope FROM users"
		}
	}
}
`, "broken")
}

func TestUnknownOptionRejected(t *testing.T) {
	loadFails(t, `
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	turso {
		db_path ":memory:"
		bogus_option yes
	}
}
`, "bogus_option")
}

func TestBackupEndpoint(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	backupDir := filepath.Join(dir, "backups")
	tc := caddytest.NewTester(t)
	tc.InitServer(fmt.Sprintf(`
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	route {
		turso {
			db_path %q
			backup_dir %q
			backup_retain 2
			schema `+"`"+`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)`+"`"+`
			query add_user {
				sql "INSERT INTO users (name) VALUES ($name) RETURNING id"
				param $name {
					from body
					key name
				}
				output {
					status 201
				}
			}
			route POST /users add_user
		}
	}
}
`, dbPath, backupDir), "caddyfile")

	code, body := do(t, tc, "POST", "http://localhost:9080/users",
		`{"name":"Ada"}`, "Content-Type", "application/json")
	if code != http.StatusCreated {
		t.Fatalf("insert: expected 201, got %d; body: %s", code, body)
	}

	var first, last string
	for i := 0; i < 3; i++ {
		code, body = do(t, tc, "POST", "http://localhost:9080/_turso/backup", "")
		if code != http.StatusOK {
			t.Fatalf("backup %d: expected 200, got %d; body: %s", i, code, body)
		}
		var res struct {
			Backup string `json:"backup"`
			Bytes  int64  `json:"bytes"`
		}
		if err := json.Unmarshal([]byte(body), &res); err != nil {
			t.Fatalf("backup %d: bad json %s: %v", i, body, err)
		}
		if res.Bytes == 0 || res.Backup == "" {
			t.Fatalf("backup %d: empty result %s", i, body)
		}
		if i == 0 {
			first = res.Backup
		}
		last = res.Backup
		t.Logf("backup: %s (%d bytes)", res.Backup, res.Bytes)
	}

	// Retention: backup_retain 2, so the first backup must be gone.
	if _, err := os.Stat(first); !os.IsNotExist(err) {
		t.Fatalf("expected %s to be pruned, stat err: %v", first, err)
	}

	// The snapshot is a standalone SQLite file that still contains our row.
	bdb, err := sql.Open("turso", last)
	if err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()
	var n int
	if err := bdb.QueryRow("SELECT COUNT(*) FROM users WHERE name = 'Ada'").Scan(&n); err != nil {
		t.Fatalf("open backup snapshot: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected Ada in backup, got %d rows", n)
	}

	_, body = do(t, tc, "GET", "http://localhost:9080/_turso/stats", "")
	if !strings.Contains(body, `"backup_count":3`) {
		t.Fatalf("expected backup_count:3 in stats, got: %s", body)
	}
}

func TestRestoreEndpoint(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	backupDir := filepath.Join(dir, "backups")
	tc := caddytest.NewTester(t)
	tc.InitServer(fmt.Sprintf(`
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	route {
		turso {
			db_path %q
			backup_dir %q
			schema `+"`"+`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)`+"`"+`
			query add_user {
				sql "INSERT INTO users (name) VALUES ($name) RETURNING id"
				param $name {
					from body
					key name
				}
				output {
					status 201
				}
			}
			query count_users {
				sql "SELECT COUNT(*) AS n FROM users"
			}
			route POST /users add_user
		}
	}
}
`, dbPath, backupDir), "caddyfile")

	jsonCT := []string{"Content-Type", "application/json"}

	// Path traversal and pattern mismatches are rejected.
	code, body := do(t, tc, "POST", "http://localhost:9080/_turso/restore",
		`{"name":"../../etc/passwd"}`, jsonCT...)
	if code != http.StatusBadRequest {
		t.Fatalf("traversal: expected 400, got %d; body: %s", code, body)
	}
	code, body = do(t, tc, "POST", "http://localhost:9080/_turso/restore",
		`{"name":"evil.db"}`, jsonCT...)
	if code != http.StatusBadRequest {
		t.Fatalf("bad pattern: expected 400, got %d; body: %s", code, body)
	}
	// Well-formed name that does not exist.
	code, body = do(t, tc, "POST", "http://localhost:9080/_turso/restore",
		`{"name":"app.db-20990101-000000.db"}`, jsonCT...)
	if code != http.StatusNotFound {
		t.Fatalf("missing backup: expected 404, got %d; body: %s", code, body)
	}

	mustInsert := func(name string) {
		code, body = do(t, tc, "POST", "http://localhost:9080/users",
			fmt.Sprintf(`{"name":%q}`, name), jsonCT...)
		if code != http.StatusCreated {
			t.Fatalf("insert %s: expected 201, got %d; body: %s", name, code, body)
		}
	}
	countIs := func(want int) {
		code, body = do(t, tc, "GET", "http://localhost:9080/_turso/query/count_users", "")
		if code != http.StatusOK || !strings.Contains(body, fmt.Sprintf(`"n":%d`, want)) {
			t.Fatalf("count: expected %d, got %d; body: %s", want, code, body)
		}
	}

	mustInsert("Ada")
	code, body = do(t, tc, "POST", "http://localhost:9080/_turso/backup", "")
	if code != http.StatusOK {
		t.Fatalf("backup: expected 200, got %d; body: %s", code, body)
	}
	var res struct {
		Backup string `json:"backup"`
	}
	if err := json.Unmarshal([]byte(body), &res); err != nil {
		t.Fatalf("backup: bad json %s: %v", body, err)
	}
	snapshot := filepath.Base(res.Backup)

	// Post-backup changes must disappear after the restore.
	mustInsert("Bob")
	countIs(2)

	code, body = do(t, tc, "POST", "http://localhost:9080/_turso/restore",
		fmt.Sprintf(`{"name":%q}`, snapshot), jsonCT...)
	if code != http.StatusOK {
		t.Fatalf("restore: expected 200, got %d; body: %s", code, body)
	}
	if !strings.Contains(body, "safety_backup") {
		t.Fatalf("restore: expected safety_backup in response, got: %s", body)
	}
	countIs(1)

	// The backup list shows the snapshot we restored from plus the
	// pre-restore safety snapshot of the state we left behind.
	code, body = do(t, tc, "GET", "http://localhost:9080/_turso/backups", "")
	if code != http.StatusOK || !strings.Contains(body, snapshot) || !strings.Contains(body, "-pre-restore") {
		t.Fatalf("backups list: got %d; body: %s", code, body)
	}

	_, body = do(t, tc, "GET", "http://localhost:9080/_turso/stats", "")
	if !strings.Contains(body, `"restores":1`) {
		t.Fatalf("expected restores:1 in stats, got: %s", body)
	}
}

func TestLogBufferDropped(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "log.db")
	tc := caddytest.NewTester(t)
	tc.InitServer(fmt.Sprintf(`
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	route {
		turso {
			db_path %q
			log_requests
			buffer_size 1
			flush_interval 10s
		}
		respond "fallback" 404
	}
}
`, dbPath), "caddyfile")

	for i := 0; i < 6; i++ {
		do(t, tc, "GET", fmt.Sprintf("http://localhost:9080/req-%d", i), "")
	}

	_, body := do(t, tc, "GET", "http://localhost:9080/_turso/stats", "")
	var stats struct {
		LogBuffer struct {
			Depth   int64 `json:"depth"`
			Dropped int64 `json:"dropped"`
		} `json:"log_buffer"`
	}
	if err := json.Unmarshal([]byte(body), &stats); err != nil {
		t.Fatalf("bad stats json: %v\n%s", err, body)
	}
	if stats.LogBuffer.Dropped == 0 {
		t.Fatalf("expected dropped > 0 with buffer_size 1 and 6 requests, got: %s", body)
	}
}

func TestTextBodyOnlyWhenEmpty(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "text.db")
	tc := caddytest.NewTester(t)
	tc.InitServer(fmt.Sprintf(`
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	route {
		turso {
			db_path %q
			schema `+"`"+`CREATE TABLE IF NOT EXISTS links (code TEXT PRIMARY KEY, url TEXT NOT NULL, hits INTEGER NOT NULL DEFAULT 0)`+"`"+`

			query visit {
				sql "UPDATE links SET hits = hits + 1 WHERE code = $code RETURNING url"
				param $code {
					from path
					key code
				}
				output {
					format text
					body "no such link"
				}
			}

			query seed {
				sql "INSERT INTO links (code, url, hits) VALUES ('a', 'https://example.com', 0)"
				output {
					format text
				}
			}

			route GET /seed seed
			route GET /go/:code visit
		}
		respond "fallback" 404
	}
}
`, dbPath), "caddyfile")

	do(t, tc, "GET", "http://localhost:9080/seed", "")
	_, body := do(t, tc, "GET", "http://localhost:9080/go/a", "")
	if strings.TrimSpace(body) != "https://example.com" {
		t.Fatalf("expected returned url text, got fallback body: %q", body)
	}
	_, body = do(t, tc, "GET", "http://localhost:9080/go/missing", "")
	if strings.TrimSpace(body) != "no such link" {
		t.Fatalf("expected fallback body for unknown code, got: %q", body)
	}
}

func TestLogsMatchedRoutes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "logroute.db")
	tc := caddytest.NewTester(t)
	tc.InitServer(fmt.Sprintf(`
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	route {
		turso {
			db_path %q
			schema `+"`"+`CREATE TABLE IF NOT EXISTS hits (n INTEGER)`+"`"+`

			query bump {
				sql "INSERT INTO hits (n) VALUES (1)"
				output {
					format text
				}
			}

			route GET /bump bump
			log_requests
			flush_interval 100ms
			raw_sql
		}
		respond "fallback" 404
	}
}
`, dbPath), "caddyfile")

	do(t, tc, "GET", "http://localhost:9080/bump", "")
	do(t, tc, "GET", "http://localhost:9080/bump", "")
	do(t, tc, "GET", "http://localhost:9080/bump", "")

	time.Sleep(300 * time.Millisecond)
	_, body := do(t, tc, "POST", "http://localhost:9080/_turso/sql",
		`{"sql":"SELECT count(*) AS n FROM _requests WHERE path = '/bump'"}`,
		"Content-Type", "application/json")
	if !strings.Contains(body, `"n":3`) {
		t.Fatalf("expected 3 logged requests for matched route, got: %s", body)
	}
}

func TestBadParamIs400(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "p400.db")
	tc := caddytest.NewTester(t)
	tc.InitServer(fmt.Sprintf(`
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	route {
		turso {
			db_path %q
			schema `+"`"+`CREATE TABLE IF NOT EXISTS t (slug TEXT)`+"`"+`

			query add {
				sql "INSERT INTO t (slug) VALUES ($slug)"
				param $slug {
					from body
					key slug
					pattern ^[a-z]{3,8}$
				}
				output {
					status 201
				}
			}

			route POST /t add
		}
		respond "fallback" 404
	}
}
`, dbPath), "caddyfile")

	code, body := do(t, tc, "POST", "http://localhost:9080/t", `{"slug":"ab"}`,
		"Content-Type", "application/json")
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for pattern violation, got %d; body: %s", code, body)
	}
	code, body = do(t, tc, "POST", "http://localhost:9080/t", `{"slug":"abc"}`,
		"Content-Type", "application/json")
	if code != http.StatusCreated {
		t.Fatalf("expected 201 for valid slug, got %d; body: %s", code, body)
	}
}

func TestColumnOrderPreserved(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "cols.db")
	tc := caddytest.NewTester(t)
	tc.InitServer(fmt.Sprintf(`
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	route {
		turso {
			db_path %q
			raw_sql
			schema `+"`"+`CREATE TABLE IF NOT EXISTS t (a TEXT, b TEXT)`+"`"+`

			# note: b before a — output must keep SELECT order, not
			# alphabetical or random map order
			query pair_text {
				sql "SELECT b, a FROM t"
				output {
					format text
				}
			}

			query pair_csv {
				sql "SELECT b, a FROM t"
				output {
					format csv
				}
			}

			route GET /text pair_text
			route GET /csv pair_csv
		}
		respond "fallback" 404
	}
}
`, dbPath), "caddyfile")

	do(t, tc, "POST", "http://localhost:9080/_turso/sql",
		`{"sql":"INSERT INTO t (a, b) VALUES ('va', 'vb')"}`,
		"Content-Type", "application/json")

	code, body := do(t, tc, "GET", "http://localhost:9080/text", "")
	if code != http.StatusOK || body != "vb\tva\n" {
		t.Fatalf("text: expected %q, got %d %q", "vb\tva\n", code, body)
	}

	code, body = do(t, tc, "GET", "http://localhost:9080/csv", "")
	if code != http.StatusOK {
		t.Fatalf("csv: got %d; body: %s", code, body)
	}
	lines := strings.Split(strings.TrimRight(body, "\n"), "\n")
	if len(lines) != 2 || lines[0] != "b,a" || lines[1] != "vb,va" {
		t.Fatalf("csv: unexpected output: %q", body)
	}
}

// Regression: a param with `default ""` must bind as an empty STRING, not
// NULL, so optional-filter predicates like `($s = ” OR s = $s)` match all
// rows when the param is absent. Absent-without-default still binds NULL.
func TestEmptyStringDefaultBindsEmptyString(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "empty.db")
	tc := caddytest.NewTester(t)
	tc.InitServer(fmt.Sprintf(`
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	route {
		turso {
			db_path %q

			schema `+"`"+`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`+"`"+`

			query add_item {
				sql "INSERT INTO items (name) VALUES ($name) RETURNING id, name"
				param $name {
					from body
					key name
				}
				output {
					status 201
					envelope
				}
			}

			query list_items {
				sql "SELECT id, name FROM items WHERE ($s = '' OR name = $s) ORDER BY id"
				param $s {
					from query
					key s
					default ""
				}
				output {
					envelope
				}
			}

			route POST /items add_item
			route GET /items list_items
		}
		respond "fallback" 404
	}
}
`, dbPath), "caddyfile")

	for _, name := range []string{"a", "b"} {
		code, body := do(t, tc, "POST", "http://localhost:9080/items",
			fmt.Sprintf(`{"name":%q}`, name), "Content-Type", "application/json")
		if code != http.StatusCreated {
			t.Fatalf("insert %q: got %d; body: %s", name, code, body)
		}
	}

	type env struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
		Meta struct {
			Count int `json:"count"`
		} `json:"meta"`
	}

	// No param and explicit-empty param share the "" value: both list all rows.
	for _, uri := range []string{"http://localhost:9080/items", "http://localhost:9080/items?s="} {
		code, body := do(t, tc, "GET", uri, "")
		if code != http.StatusOK {
			t.Fatalf("GET %s: got %d; body: %s", uri, code, body)
		}
		var e env
		if err := json.Unmarshal([]byte(body), &e); err != nil {
			t.Fatalf("GET %s: bad json %q: %v", uri, body, err)
		}
		if e.Meta.Count != 2 || len(e.Data) != 2 {
			t.Fatalf("GET %s: expected 2 rows, got %d (data %d); body: %s", uri, e.Meta.Count, len(e.Data), body)
		}
	}

	// A concrete value filters.
	code, body := do(t, tc, "GET", "http://localhost:9080/items?s=a", "")
	if code != http.StatusOK {
		t.Fatalf("filtered: got %d; body: %s", code, body)
	}
	var e env
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		t.Fatalf("filtered: bad json %q: %v", body, err)
	}
	if e.Meta.Count != 1 || len(e.Data) != 1 || e.Data[0].Name != "a" {
		t.Fatalf("filtered: expected only 'a', body: %s", body)
	}

	// Literal "null" still binds NULL and matches nothing.
	code, body = do(t, tc, "GET", "http://localhost:9080/items?s=null", "")
	if code != http.StatusOK {
		t.Fatalf("null: got %d; body: %s", code, body)
	}
	e = env{}
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		t.Fatalf("null: bad json %q: %v", body, err)
	}
	if e.Meta.Count != 0 {
		t.Fatalf("null: expected 0 rows, got %d; body: %s", e.Meta.Count, body)
	}
}

func TestWriteInvalidatesCache(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "cacheinv.db")
	tc := caddytest.NewTester(t)
	tc.InitServer(fmt.Sprintf(`
{
	admin localhost:2999
	http_port 9080
}

localhost:9080 {
	route {
		turso {
			db_path %q

			schema `+"`"+`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`+"`"+`

			query add_item {
				sql "INSERT INTO items (name) VALUES ($name) RETURNING id"
				param $name {
					from body
					key name
				}
				output {
					status 201
					envelope
				}
			}

			query list_items {
				sql "SELECT id, name FROM items ORDER BY id"
				cache 10s
				output {
					envelope
				}
			}

			query rename_item {
				sql "UPDATE items SET name = $name WHERE id = $id RETURNING id"
				param $name {
					from body
					key name
				}
				param $id {
					from path
					key id
					type int
					min 1
				}
				output {
					envelope
				}
			}

			route POST /items add_item
			route GET /items list_items
			route PUT /items/:id rename_item
		}
		respond "fallback" 404
	}
}
`, dbPath), "caddyfile")

	type env struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}

	code, body := do(t, tc, "POST", "http://localhost:9080/items",
		`{"name":"old"}`, "Content-Type", "application/json")
	if code != http.StatusCreated {
		t.Fatalf("insert: got %d; body: %s", code, body)
	}

	// Two identical reads: the second is served from the 10s cache.
	for i := 0; i < 2; i++ {
		code, body = do(t, tc, "GET", "http://localhost:9080/items", "")
		if code != http.StatusOK || !strings.Contains(body, "old") {
			t.Fatalf("read %d: got %d; body: %s", i, code, body)
		}
	}

	// A successful write must drop the cached result.
	code, body = do(t, tc, "PUT", "http://localhost:9080/items/1",
		`{"name":"new"}`, "Content-Type", "application/json")
	if code != http.StatusOK {
		t.Fatalf("rename: got %d; body: %s", code, body)
	}

	code, body = do(t, tc, "GET", "http://localhost:9080/items", "")
	if code != http.StatusOK {
		t.Fatalf("read after write: got %d; body: %s", code, body)
	}
	var e env
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		t.Fatalf("bad json %q: %v", body, err)
	}
	if len(e.Data) != 1 || e.Data[0].Name != "new" {
		t.Fatalf("expected fresh row 'new' immediately after write, got %s", body)
	}

	// The invalidation is observable in stats (insert + rename = 2 writes).
	code, body = do(t, tc, "GET", "http://localhost:9080/_turso/stats", "")
	if code != http.StatusOK || !strings.Contains(body, `"cache_invalidations":2`) {
		t.Fatalf("stats: got %d; body: %s", code, body)
	}
}
