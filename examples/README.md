# caddy-turso examples

Self-contained, real-life-like examples. Each directory holds a `Caddyfile`
(plus any SQL files it references) and a `README.md` with a full walkthrough.

| Example                             | Shows                                                                          |
| ----------------------------------- | ------------------------------------------------------------------------------ |
| [crud-api](crud-api/)               | full REST CRUD: path/body/query params, validation, cache, rate limits, CORS   |
| [backups](backups/)                 | periodic + on-demand `VACUUM INTO` backups, retention, restore                 |
| [sync](sync/)                       | local replica CDC-synced with a remote Turso database                          |
| [request-logging](request-logging/) | `:memory:` DB, DSN options, `sql_file`, `_requests` analytics, buffer/overflow |

## Running an example

```sh
# from the repo root, build once:
./build.sh                     # produces ./caddy-turso

# then, from inside an example directory:
cd examples/crud-api
../../caddy-turso run --config Caddyfile --adapter caddyfile
```

Database files, backups and logs are created next to the Caddyfile (relative
paths are resolved against caddy's working directory).

Each example also serves a small web UI at <http://localhost:8080/>
(`index.html` via `file_server`) that exercises the same routes from a page —
handy for poking at a demo without curl.

Every example exposes the management API under `/_turso` (protected by
`api_token change-me`):

```sh
curl -H "Authorization: Bearer change-me" localhost:8080/_turso/stats
```
