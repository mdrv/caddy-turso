# sync — edge replica with a remote Turso database

The local file (`edge.db`) serves every read and write at local speed, while
a background loop pushes and pulls changes via Turso's CDC-based replication.
Run several edges against the same remote database and they converge — each
with its own `sync_client` name.

## Remote setup (Turso Cloud, free tier works)

```sh
turso db create edge-demo

export TURSO_SYNC_URL=$(turso db show edge-demo --url)
export TURSO_AUTH_TOKEN=$(turso db tokens create edge-demo)
```

## Run

```sh
./build.sh                     # repo root, once
cd examples/sync
../../caddy-turso run --config Caddyfile --adapter caddyfile
```

On first start the local file bootstraps from the remote (if the remote is
empty, the schema creates the `notes` table and it replicates up).

## Web UI

Open <http://localhost:8080/> — post notes, trigger a manual sync, and watch
replication stats (revision, network bytes, last sync). Without the env vars
above the server runs standalone and the sync button reports the 412.

## Walkthrough

```sh
curl -s -X POST localhost:8080/api/notes \
  -H 'Content-Type: application/json' \
  -d '{"body": "hello from the edge"}'
```

`origin` comes from the `EDGE_NAME` env var (`edge-1` by default) — set it
differently per machine to trace where notes were created.

```sh
curl -s localhost:8080/api/notes
curl -s localhost:8080/api/notes/1
```

Writes replicate up (and other clients' writes down) on the next
`sync_interval` tick, or immediately:

```sh
curl -s -X POST -H 'Authorization: Bearer change-me' localhost:8080/_turso/sync
# {"status":"synced"}   (HTTP 412 if sync_url is not configured)
```

## Observing replication

```sh
curl -s -H 'Authorization: Bearer change-me' localhost:8080/_turso/stats
```

```json
{
	"last_sync": "2026-08-16T12:00:15Z",
	"last_sync_ok": true,
	"sync_errors": 0,
	"sync": {
		"cdc_operations": 12,
		"last_pull_unix_time": 1786886415,
		"last_push_unix_time": 1786886415,
		"network_received_bytes": 8192,
		"network_sent_bytes": 2048,
		"revision": 7
	}
}
```

## Notes

- Reads and writes never wait on the network — only the background loop
  (and `POST /_turso/sync`) touch the remote.
- The local file is still single-process-per-file; one caddy per machine,
  and use distinct `db_path`s if you run multiple instances on a host.
- Without the env vars set, config load fails with a clear error at
  provision (`sync_token is required...` / unresolved placeholders).
