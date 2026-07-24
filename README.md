# ee-database

Self-hosted apply client for [Entity Enricher](https://entityenricher.ai) schema databases.

Entity Enricher can mirror every enrichment into **your own database** as plain
relational tables. This CLI runs next to that database, connects **outward**
over WSS (no inbound access, no credentials shared with Entity Enricher),
receives delta batches, applies them and acknowledges:

```
Entity Enricher ──WSS──▶ ee-database ──SQL──▶ your PostgreSQL / MySQL / SQLite
        ◀──────ack───────┘
```

- **Bootstrap included** — on first run it fetches the `.sql` snapshot (tables +
  current data) and applies it before consuming deltas.
- **Replay-safe** — every delta is an idempotent, revision-guarded upsert; if the
  client dies mid-batch, the batch is redelivered after its lease expires and
  re-applying converges to the same rows.
- **Your DSN stays local** — the connection string is passed on the command line
  or stored mode-600 on the machine; it is never sent anywhere.
- **Halt on failure** — a SQL error rolls back the batch, reports the failing
  delta to the Entity Enricher UI (Databases page → Sync client), and exits so
  a poison delta can never be silently skipped.
- **Several databases per machine** — each pairing lives in its own local
  profile; `run --all` syncs every paired database concurrently from one
  process.

## Install

Download a signed binary from [Releases](https://github.com/TOT-Concept/ee-database/releases),
or build from source (Go ≥ 1.23):

```bash
go build -o ee-database .
```

For an **editable install** while hacking on the CLI, symlink the build output
onto your `PATH` so every rebuild is instantly live:

```bash
mkdir -p ~/.local/bin
go build -o tmp/ee-database .
ln -sfn "$PWD/tmp/ee-database" ~/.local/bin/ee-database   # ensure ~/.local/bin is on PATH
```

## Quick start

```bash
# 1. Pair with your Entity Enricher organization (opens the browser to pick the database)
ee-database pair --server https://entityenricher.ai

# 2. Run it next to your database
ee-database run --dsn "postgres://user:pass@localhost:5432/mydb" --save-dsn
```

Alternatively, issue a token on the Databases page (Sync client → Pair a client)
and paste it: `ee-database pair --server https://entityenricher.ai <refresh-token>`.

If the target database does not exist yet, add `--create-missing` to the `run`
command: the client creates it with the DSN's own credentials before syncing
(postgres: needs `CREATEDB`, goes through the standard `postgres` maintenance
database; mysql: needs the `CREATE` privilege; sqlite: unnecessary — the file
is created automatically).

When even the DSN's role/user does not exist yet, add `--admin-dsn` with an
administrative connection: the client then bootstraps everything the target
DSN names — the role (with the target DSN's password, postgres) or user
(mysql, plus a grant on the database) when missing, and the database owned by
it. The target DSN needs no create rights of its own in this mode:

```bash
ee-database run \
  --dsn "postgres://ee_replica:secret@localhost:5432/my_replica" \
  --create-missing --admin-dsn "postgres://postgres:admin@localhost:5432/postgres"
```

The admin DSN is used only for this bootstrap — it is never stored.

## Commands

| Command | Purpose |
|---|---|
| `pair --server URL` | Browser-confirmed device-code pairing |
| `pair --server URL <token>` | Pair with a token from the Databases page |
| `run --dsn DSN [--save-dsn] [--skip-bootstrap]` | Connect and apply deltas |
| `run … --create-missing` | Create the target database first when it does not exist (postgres/mysql; sqlite files are created anyway) |
| `run … --create-missing --admin-dsn DSN` | Also create the target DSN's missing role/user via an admin connection (never stored) |
| `run --all` | Sync every paired database concurrently (each needs a saved DSN) |
| `status` | Show pairing state (all paired databases) |
| `disconnect` | Forget one pairing's local credentials (revoke server-side in the UI) |
| `version` | Print version |

## Multiple databases

Pair once per schema database — every pairing gets its own profile directory
(`~/.config/ee-database/profiles/<database-id>/` on Linux, the OS config dir
elsewhere), so pairing a second database never disturbs the first.

When several databases are paired, `run`, `status` and `disconnect` accept
`--database NAME|ID` to pick one. Without the flag, a terminal prompts with a
numbered list; a non-interactive run (systemd, Docker, cron) fails with the
list instead of hanging — pass `--database` there, or use `run --all`:

```bash
# Save each DSN once…
ee-database run --database analytics --dsn "postgres://…/analytics" --save-dsn
ee-database run --database staging   --dsn "postgres://…/staging"   --save-dsn

# …then one process syncs both (log lines are prefixed per database)
ee-database run --all
```

A per-profile lock prevents two processes from running the same pairing at the
same time (they would evict each other's WebSocket session in a loop).

Pairings made with ee-database ≤ 0.1.x are migrated into a profile
automatically on first use.

## Dialects

The target dialect is fixed by the schema database registration in Entity
Enricher. PostgreSQL is the launch dialect; the MySQL and SQLite drivers are
already bundled for when their SQL renderers ship.

## Security model

- One credential = one schema database = one client. Pairing again rotates the
  credential; the old token stops working immediately.
- The refresh token (365 days) is stored mode-600 in the pairing's profile
  directory; it is exchanged for 15-minute access tokens that authenticate the
  WebSocket.
- Revoking the credential in the UI disconnects a live client instantly.
- Recommended: run against a dedicated database role scoped to the synced
  schema.

## License

MIT — see [LICENSE](LICENSE).
