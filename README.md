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

```sh
curl -fsSL https://entityenricher.ai/install-eedatabase.sh | sh
```

(Windows: `iwr -useb https://entityenricher.ai/install-eedatabase.ps1 | iex`.
Both URLs are 302 aliases of [`install.sh`](install.sh) /
[`install.ps1`](install.ps1) at the root of this repo.)

The installer prints what it's about to do, pauses 5 seconds, and verifies the
binary's **Sigstore keyless signature** before making it executable: releases
are signed in CI by this repo's `release.yml` GitHub OIDC identity and logged
in the public [Rekor](https://search.sigstore.dev) transparency log — there is
no long-lived signing key anywhere.

Alternatively, download a signed binary from
[Releases](https://github.com/TOT-Concept/ee-database/releases) yourself,
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

On every run the client self-checks its login's provisioning rights — create
database (from the catalog), DDL and DML (a create/insert/delete/drop probe
table) — prints the result and reports it to the Databases page's Sync client
card, so a missing grant is visible there before deltas fail to apply. The
check is advisory: it never blocks the run.

If none of the database's linked schemas is published yet, the first run has
nothing to bootstrap: the client stays connected and waits, and the first
publish starts the feed on its own — no restart needed.

## Commands

| Command | Purpose |
|---|---|
| `pair --server URL` | Browser-confirmed device-code pairing |
| `pair --server URL <token>` | Pair with a token from the Databases page |
| `run --dsn DSN [--save-dsn] [--skip-bootstrap]` | Connect and apply deltas |
| `run … --create-missing` | Create the target database first when it does not exist (postgres/mysql; sqlite files are created anyway) |
| `run … --create-missing --admin-dsn DSN` | Also create the target DSN's missing role/user via an admin connection (never stored) |
| `run --all` | Sync every paired database concurrently (each needs a saved DSN) |
| `host pair --server URL --dsn BASE_DSN [--admin-dsn DSN] <token>` | Pair this machine as a managed sync host (token from the Sync hosts dialog — the "Sync hosts" toolbar button on the Database Sync page) |
| `host run` | Managed mode: auto-claim, create and sync every database sync assigned to this host |
| `host status` / `host disconnect` | Show / forget the host pairing |
| `status` | Show pairing state (all paired databases) |
| `disconnect` | Forget one pairing's local credentials (revoke server-side in the UI) |
| `version` | Print version |

## Managed host mode

Pairing is per database; managed mode moves the ceremony one level up. Pair the
machine once — `--dsn` is a **base server DSN with no database name** (it never
leaves the machine) — then `host run` holds one control-plane WebSocket and
reacts to assignments made in the Entity Enricher UI (or API): it claims the
credential, derives the database's DSN from the base DSN plus the
server-suggested snake_cased name (override per registration via
`database_names` in the host `config.json`), creates the database if missing,
runs the rights preflight, and starts the ordinary sync loop. A database
already paired with another client is reported and skipped — never hijacked.
Revoking the host in the UI cuts the machine off entirely, including every
credential it claimed.

```bash
# 1. Register a host on the Database Sync page ("Sync hosts" toolbar button →
#    Add host) — the one-time token is shown embedded in this command:
ee-database host pair --server https://entityenricher.ai \
  --dsn "postgres://ee_replica:secret@dbserver:5432/" \
  --admin-dsn "postgres://postgres:admin@dbserver:5432/postgres" <token>

# 2. Keep it running; assign database syncs to the host in the UI
ee-database host run
```

`--admin-dsn` plays the same role as on `run --create-missing`: provisioning
creates the target role and each claimed database through the admin
connection, so the base DSN's login needs no create rights of its own. It is
used at provision time only — never stored. Without it, pairing preflights the
base DSN's own rights and fails fast on a missing `CREATEDB`
(`--skip-preflight` to override; `host run` re-checks and reports to the Sync
hosts card either way).

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

## Troubleshooting a failed apply

When the target database rejects the snapshot or a delta, ee-database prints
everything the database reported — SQLSTATE, detail, hint, and (when the
server provides an error cursor) the offending script line. A failed snapshot
is additionally saved to the pairing's profile directory as
`snapshot-failed.sql` (mode 0600, replaced on each attempt, removed on the
next success), so the failing statement can be inspected or replayed with
`psql -f`. Nothing is partially applied: the snapshot is one transaction, and
a failed delta batch rolls back and is redelivered once the cause is fixed.

## Security model

- One credential = one schema database = one client. Pairing again rotates the
  credential; the old token stops working immediately.
- The refresh token (365 days) is stored mode-600 in the pairing's profile
  directory; it is exchanged for 15-minute access tokens that authenticate the
  WebSocket.
- The host pairing key (`eeh_…`) is a short opaque secret, not a JWT: the
  server keeps only its hash, it never expires, and revoking the host in the
  UI is what ends it.
- Revoking the credential in the UI disconnects a live client instantly.
- Recommended: run against a dedicated database role scoped to the synced
  schema.

## License

MIT — see [LICENSE](LICENSE).
