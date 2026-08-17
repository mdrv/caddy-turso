# backups — URL shortener with automatic snapshots

A small URL shortener whose database is snapshotted every hour with
`VACUUM INTO` (online, consistent, no downtime), with retention pruning and
on-demand backups via the management API. Also shows the `_requests` table
(the built-in request log) queried through a regular route.

## Run

```sh
./build.sh                     # repo root, once
cd examples/backups
../../caddy-turso run --config Caddyfile --adapter caddyfile
```

Creates `links.db` and a `backups/` directory next to the Caddyfile.

## Web UI

Open <http://localhost:8080/> — shorten links, watch hit counters climb,
trigger a backup, **list snapshots and restore them with one click**, and
see the `_requests` log fill up, all from one page.

## Walkthrough

### Shorten a link (validated body params)

```sh
curl -s -X POST localhost:8080/api/links \
  -H 'Content-Type: application/json' \
  -d '{"code": "caddy", "url": "https://caddyserver.com"}'
```

`code` must match `^[a-z0-9_-]{3,32}$`, `url` must be an http(s) URL.

### Visit (path param, text output, counter update)

```sh
curl -s localhost:8080/s/caddy
# https://caddyserver.com        (raw text; hits incremented)

curl -s localhost:8080/s/nope
# no such link
```

### Stats

```sh
curl -s localhost:8080/api/links
# {"data":[{"code":"caddy","hits":1,"url":"https://caddyserver.com"}],"meta":{...}}
```

### Request log as a route

Every request (except `/_turso*`) lands in `_requests` within ~1s:

```sh
curl -s 'localhost:8080/api/recent?limit=5'
```

## Backups

Automatic: `backup_interval 1h`, `backup_retain 24` — the newest 24
snapshots are kept, older ones pruned.

On demand:

```sh
curl -s -X POST -H 'Authorization: Bearer change-me' localhost:8080/_turso/backup
# {"backup":"backups/links-20260816-120000.db","bytes":20480}

curl -s -H 'Authorization: Bearer change-me' localhost:8080/_turso/backups
# {"backups":[{"name":"links-20260816-120000.db","bytes":20480,"modified":"..."}],
#  "count":1}

curl -s -H 'Authorization: Bearer change-me' localhost:8080/_turso/stats
# last_backup, last_backup_bytes, backup_count, backup_errors, restores,
# log_buffer{...}
```

Snapshots are **standalone SQLite files** taken while the DB serves traffic:

```sh
sqlite3 backups/links-*.db 'SELECT code, url, hits FROM links;'
```

### Restoring

**Without restarting** — swap the live database for any snapshot (a
safety snapshot of the current state is written first, so a bad restore
can be undone by restoring the `-pre-restore` file):

```sh
curl -s -X POST -H 'Authorization: Bearer change-me' \
  -H 'Content-Type: application/json' \
  -d '{"name": "links-20260816-120000.db"}' \
  localhost:8080/_turso/restore
# {"bytes":20480,"restored":"links-20260816-120000.db",
#  "safety_backup":"links-20260816-130102-pre-restore.db"}
```

The web UI does the same via the **restore** button next to each
snapshot.

The offline way still works (stop caddy first):

```sh
rm -f links.db links.db-wal
cp backups/links-20260816-120000.db links.db
# start caddy again
```

(While caddy runs, the WAL is checkpointed every 10 minutes and on clean
shutdown, so `links.db` on disk is never far behind.)
