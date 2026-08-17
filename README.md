# caddy-turso

A [Caddy](https://caddyserver.com) HTTP handler module backed by the
[Turso database engine](https://turso.tech/database) — a Rust rewrite of
SQLite — via the purego `tursogo` driver (no CGO, no system dependencies;
the native library is embedded in the binary).

Define queries and routes in your Caddyfile; caddy serves them as JSON
(or CSV/NDJSON/text) and can write to the database through HTTP. Works
fully offline with a local file or `:memory:`, and optionally syncs with
a remote Turso instance using CDC-based replication.

## Build

```sh
./build.sh          # produces ./caddy-turso (a caddy binary with the module)
```

## Quick start

```
example.com {
	route {
		turso {
			db_path /var/lib/caddy/app.db

			schema `
				CREATE TABLE IF NOT EXISTS hits (
					id INTEGER PRIMARY KEY,
					path TEXT NOT NULL,
					ts TEXT NOT NULL
				)
			`

			query recent {
				sql "SELECT path, count(*) AS n FROM hits GROUP BY path ORDER BY n DESC LIMIT $limit"
				param $limit {
					from query
					key limit
					type int
					default 10
					max 100
				}
				cache 5s
			}

		query record {
			sql "INSERT INTO hits (path, ts) VALUES ($path, datetime('now'))"
			param $path {
				from query
				key path
			}
		}

			route GET  /api/recent  recent
			route POST /api/hit     record
		}
	}
}
```

## Directive reference

```
turso {
	# required; supports ":memory:" and DSN params: app.db?_busy_timeout=10000
	db_path <path>

	# how long to retry opening during a graceful reload while the old
	# process still holds the file lock (default 15s)
	open_timeout <duration>

	# max concurrent connections (default 4)
	max_conns <n>

	# PRAGMA statements run at open (can repeat). "pragma name value" is
	# normalized to "PRAGMA name = value"; full statements work too:
	# pragma "PRAGMA synchronous = NORMAL"
	pragma <name> <value...>

	# DDL run at provision; one backtick-quoted statement or "file:<path>"
	# (can repeat)
	schema `<stmt>`

	# named query (see below; can repeat)
	query <name> { ... }

	# route (can repeat)
	route <METHOD> <path> <query_name> {
		require_header <name> <value>
		rate_limit <n> per <duration>
	}

	# log requests into the local _requests table (async batch writer)
	log_requests [on|off]
	exclude_path <glob>
	batch_size <n>              # rows per multi-row INSERT (default 500)
	flush_interval <duration>   # default 200ms
	buffer_size <n>             # max pending rows, chan + batch (default 8192)
	overflow <drop|block>       # when the buffer is full: drop+count, or
	                            # apply backpressure (default drop)

	# periodic WAL checkpoint (TRUNCATE); 0 = disabled
	checkpoint <duration>

	# online backups via VACUUM INTO: timestamped standalone-SQLite
	# snapshots in backup_dir (can also be triggered on demand, see below)
	backup_dir <path>
	backup_interval <duration>  # default 6h when backup_dir is set
	backup_retain <n>           # keep the newest n snapshots (0 = keep all)

	# optional sync with a remote Turso database
	sync_url <url>
	sync_token <token>
	sync_interval <duration>    # default 30s
	sync_client <name>          # default caddy-turso

	# debugging / performance
	slow_query <duration>       # warn above; default 200ms, 0 = off
	max_rows <n>                # row cap per query; default 10000

	# management API (default /_turso)
	api_path <path>
	api_token <token>
	raw_sql [on|off]            # enable POST /_turso/sql
	cors_origin <origin>
}
```

### Queries

```
query <name> {
	sql "SELECT ... WHERE x = $x"     # named params, $name
	sql_file <path>

	param $x {
		from <query|header|body|path|env|placeholder>
		key <name>
		type <int|float|bool|string>
		default <value>
		min <n>  max <n>  cap <n>
		pattern <regex>
	}

	# empty-value semantics:
	#   absent + default          -> the default
	#   absent/empty + default "" -> empty STRING (so `($s = '' OR s = $s)`
	#                                 optional filters work)
	#   absent, no default        -> SQL NULL
	#   literal "null"            -> SQL NULL

	output {
		format <json|ndjson|csv|text>
		envelope [on|off]             # {"data": [...], "meta": {...}}
		alias <column> <name>          # rename output column
		omit <column>                  # drop output column
		status <code>
		body <text>                    # static body when 0 rows
	}

	cache <duration>   # TTL result cache
	timeout <duration> # per-query timeout (default 10s)
}
```

All queries are validated with `EXPLAIN QUERY PLAN` at provision time —
typos and missing columns fail the config load instead of the first request.

The TTL cache is invalidated automatically: any successful write query
(`INSERT`, `UPDATE`, `DELETE`, `REPLACE`, `CREATE`, `DROP`, `ALTER` — via
routes or the raw-SQL endpoint) drops all cached results, so a read right
after a write never returns stale data. `/_turso/stats` exposes
`cache_hits` and `cache_invalidations`.

### Routes

Paths support `:param` segments which bind to `from path` params:

```
route GET /api/users/:id get_user
```

## Management API

| Endpoint                   | Description                                                        |
| -------------------------- | ------------------------------------------------------------------ |
| `GET /_turso/health`       | ping the database                                                  |
| `GET /_turso/stats`        | counters (queries, cache hits, rows, slow queries, sync stats)     |
| `GET /_turso/queries`      | list registered query names                                        |
| `GET /_turso/query/<name>` | run a registered query                                             |
| `POST /_turso/sql`         | raw SQL (`{"sql": "...", "params": [...]}`), requires `raw_sql on` |
| `POST /_turso/sync`        | trigger a push+pull cycle (sync mode only)                         |
| `POST /_turso/backup`      | take a snapshot now (requires `backup_dir`)                        |
| `GET /_turso/backups`      | list snapshots, newest first (requires `backup_dir`)               |
| `POST /_turso/restore`     | swap in a snapshot (`{"name": ...}` or `?name=`), see below        |

### Restore

`POST /_turso/restore` replaces the live database with a snapshot from
`backup_dir` **without restarting Caddy**: it takes a safety snapshot of
the current state first (`<name>-pre-restore` in the list), swaps the
file, and reopens the database in place — so a bad restore can itself be
restored. Name validation is strict (`<db>-*.db` from `backup_dir`; no
traversal). Not available with `:memory:` or when `sync_url` is set;
in-flight queries during the swap may fail, and cached results are
dropped. Stats expose `restores` / `last_restore`.

All endpoints require `Authorization: Bearer <api_token>` when `api_token`
is set.

## Notes

- See [examples/](examples/) for complete, runnable setups: a CRUD API,
  automatic backups + restore, CDC sync with Turso Cloud, and request-log
  analytics.
- The Turso engine allows one process per database file. During `caddy
  reload` the module retries opening for `open_timeout` until the old
  process releases the lock.
- The database file is SQLite-compatible; you can inspect it with the
  `sqlite3` CLI while caddy is stopped.
- No CGO — the Rust engine is embedded per platform via go:embed.

## License

MIT
