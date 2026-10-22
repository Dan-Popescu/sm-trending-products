# PostgreSQL test engine in a restricted workspace

Recorded on 2026-10-22. This note describes an additional local verification route, not a production database choice or a replacement for native PostgreSQL integration tests.

## Environment constraint

This workspace's user namespace maps only UID 0. Native PostgreSQL correctly refuses to initialize or run as root, and the namespace cannot switch to a normal database service user. Do not patch or bypass those checks. Docker is unavailable here.

Each command execution also has its own network namespace. A server started in one execution is not reachable at the same loopback address from another execution. Start the database and clients in the same command or process tree.

## Verified alternative

Electric's [PGlite](https://pglite.dev/docs/about) runs PostgreSQL compiled to WebAssembly. Its official [socket adapter](https://pglite.dev/docs/pglite-socket) exposes PostgreSQL wire protocol over TCP for ordinary clients. The packages tested were:

| Package | Version |
| --- | --- |
| `@electric-sql/pglite` | `0.5.11` |
| `@electric-sql/pglite-socket` | `0.2.14` |

They were installed outside the repository in `/workspace/scratch/6f3ec5057b63/pglite-test-engine`. No package was added to the production Python environment or application runtime.

```bash
npm install --prefix /workspace/scratch/6f3ec5057b63/pglite-test-engine \
  @electric-sql/pglite@0.5.11 @electric-sql/pglite-socket@0.2.14
```

Start the server and test command together. The following pattern was verified with a Python `psycopg` probe:

```bash
/workspace/scratch/6f3ec5057b63/pglite-test-engine/node_modules/.bin/pglite-server \
  --db=memory:// --port=55432 --host=127.0.0.1 --max-connections=8 \
  --run '/workspace/scratch/6f3ec5057b63/sm-trending-products/.venv/bin/python /workspace/scratch/6f3ec5057b63/pglite-test-engine/probe.py'
```

The client connected to `postgresql://postgres:postgres@127.0.0.1:55432/postgres?sslmode=disable`. It executed `SELECT version()`, `SELECT pg_advisory_xact_lock(123)`, and a catalog query successfully. The returned version identified PostgreSQL 18.6 with PGlite 0.5.11. These are executed results, not inferred compatibility.

For automated tests, prefer a fixture that starts the server with `subprocess.Popen`, waits for readiness by connecting, and always terminates it in teardown. The observed CLI left the server alive after a successful `--run` command, so that flag alone is insufficient process cleanup. An in-memory instance avoids persistent state between suites.

## Limits of the evidence

- PGlite has one underlying database connection. The socket adapter multiplexes client connections; this differs from native PostgreSQL. Passing a multi-client test here does **not** validate independent backend concurrency, row-lock blocking, deadlock behavior, advisory-lock isolation, lease races, or fair `SKIP LOCKED` work claiming.
- Successful SQL execution proves compatibility only for exercised queries. It does not establish production PostgreSQL version compatibility or extension availability. The adapter does not support SSL; disable it only for this isolated local server.
- Native PostgreSQL services in CI and the eventual VPS remain the required verification environment for concurrent worker, shared-quota, fencing, backup/restore, and server-operation acceptance.
- S3-compatible tests remain a separate service boundary. Run a local object-store service or its protocol test substitute in the same command namespace as the clients, and describe that substitute accurately in test reports.

Report results with the actual engine and limitations. Do not relabel PGlite verification as a native PostgreSQL load test or mark native concurrency acceptance complete because this fallback passed.
