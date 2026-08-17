# crud-api — a task manager REST API

A realistic CRUD service backed by a local Turso database file (`tasks.db`),
showing off param binding, validation, caching, rate limiting, CORS and
request logging — with zero application code.

## Run

```sh
# from the repo root (once):
./build.sh

# then:
cd examples/crud-api
../../caddy-turso run --config Caddyfile --adapter caddyfile
```

The database file `tasks.db` is created (with the `tasks` table) on first
start. Stop with Ctrl-C; state persists across restarts.

## Web UI

Open <http://localhost:8080/> — a single static page (`index.html`, served by
`file_server`) that talks to the same API below: list/filter tasks, create,
advance status, delete, and watch the aggregate stats update.

## Walkthrough

### Create (POST, JSON body params, 201 + envelope)

```sh
curl -s -X POST localhost:8080/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{"title": "write caddy-turso examples"}'
```

```json
{
	"data": [
		{
			"created_at": "2026-08-16 12:00:00",
			"id": 1,
			"status": "todo",
			"title": "write caddy-turso examples"
		}
	],
	"meta": { "count": 1, "generated": "..." }
}
```

`status` is validated against `^(todo|doing|done)$`; a bad value is rejected:

```sh
curl -s -X POST localhost:8080/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{"title": "x", "status": "nope"}'
# {"error":"param binding: param \"status\": value \"nope\" doesn't match pattern \"^(todo|doing|done)$\""}
```

Omitted fields fall back to `default` (`status` → `todo`).

### List (query params, cache, rate limit)

```sh
curl -s 'localhost:8080/api/tasks?status=todo&limit=5&offset=0'
```

`limit` is clamped by `min 1` / `max 100` (out-of-range → 400-style error
JSON, HTTP 500). Results are cached for 2s **per distinct param set** —
`?status=todo` and `?status=done` never share an entry. The route allows
60 requests/min per client IP; beyond that you get
`{"error":"rate limited"}` with HTTP 429.

### Get one (path param)

```sh
curl -s localhost:8080/api/tasks/1
```

`:id` binds to `from path` and is coerced to `int` (`min 1`).

### Update (PUT, partial via COALESCE/NULLIF)

```sh
curl -s -X PUT localhost:8080/api/tasks/1 \
  -H 'Content-Type: application/json' \
  -d '{"status": "done"}'
```

Empty/omitted fields keep their current values.

### Delete

```sh
curl -s -X DELETE localhost:8080/api/tasks/1
# 0 rows -> the static body:  no task with that id
```

### Stats (aggregates, 5s cache)

```sh
curl -s localhost:8080/api/stats
```

## Ops

```sh
# live counters: queries, cache_hits, slow_queries, rows, query_time_us, ...
curl -s -H 'Authorization: Bearer change-me' localhost:8080/_turso/stats

# health + query registry
curl -s -H 'Authorization: Bearer change-me' localhost:8080/_turso/health
curl -s -H 'Authorization: Bearer change-me' localhost:8080/_turso/queries

# the request log itself (every non-/_turso request is recorded)
curl -s -H 'Authorization: Bearer change-me' \
  'localhost:8080/_turso/query/list_tasks?limit=3'
```

CORS is open (`cors_origin *`), so a browser SPA can call these endpoints
directly; OPTIONS preflight returns 204.

Slow queries (>100ms) are logged as warnings and counted in stats. All
queries were validated with `EXPLAIN QUERY PLAN` at config load — a typo in
any SQL fails the load, not a request.
