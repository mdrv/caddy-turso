# request-logging — traffic analytics with an in-memory DB

An event collector that also turns caddy itself into a data source: every
request the server handles (except `/_turso*` and `/healthz*`) is written to
the built-in `_requests` table and queried back through normal routes —
top paths, slowest responses, recent events. Everything lives in
`:memory:` (with DSN options) and resets on restart.

## Run

```sh
./build.sh                     # repo root, once
cd examples/request-logging    # sql_file paths are relative to here
../../caddy-turso run --config Caddyfile --adapter caddyfile
```

## Web UI

Open <http://localhost:8080/> — a live dashboard: fire (or burst) events,
and watch top paths, slowest requests, recent events and engine counters
(including log-buffer depth/drops) refresh every few seconds.

## Walkthrough

### Ingest events

```sh
curl -s -X POST localhost:8080/e \
  -H 'Content-Type: application/json' \
  -d '{"kind": "click", "path": "/pricing", "note": "hero button"}'

curl -s -X POST localhost:8080/e \
  -H 'Content-Type: application/json' \
  -d '{"kind": "error", "path": "/checkout"}'
```

`kind` is validated against `^(click|view|error)$`.

### Generate some traffic, then look at the dashboards

Any request to the server is logged — including unmatched paths, which hit
the `respond "ok" 200` fallback:

```sh
for i in $(seq 1 20); do curl -s -o /dev/null localhost:8080/some/page; done
curl -s -o /dev/null localhost:8080/healthz     # excluded from the log

sleep 1   # flush_interval is 500ms

curl -s localhost:8080/stats/top
# {"data":[{"path":"/some/page","hits":20,"avg_ms":0.3,"max_ms":1}, ...], ...}

curl -s 'localhost:8080/stats/slow?min_ms=0'
curl -s localhost:8080/events
```

`top_paths` and `slow_requests` live in separate `.sql` files (`sql_file`)
kept next to the Caddyfile — they are still validated with
`EXPLAIN QUERY PLAN` at load.

## Writer tuning

```
log_requests
batch_size 200          # rows per multi-row INSERT
flush_interval 500ms    # max time a row waits in memory
buffer_size 2048        # total pending rows (chan + batch buffer)
overflow block          # when full: wait (block) instead of dropping
```

`overflow drop` (the default) discards log rows under sustained overload
and counts them in stats; `overflow block` applies backpressure so nothing
is lost, at the cost of request latency. Watch both:

```sh
curl -s -H 'Authorization: Bearer change-me' localhost:8080/_turso/stats
# "log_buffer": {"depth": 0, "dropped": 0}
```
