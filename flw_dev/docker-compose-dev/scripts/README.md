# Dev-stack helper scripts

Helpers for building the Trino fork image and driving the local Lakekeeper + MinIO + Trino stack
(`../docker-compose.yml`). All scripts are self-locating — run them from anywhere.

| Script | What it does |
| --- | --- |
| `env.sh` | Common paths, `JAVA_HOME`, Maven flags, Trino URL/creds. Sourced by the others; not run directly. |
| `build-image.sh` | Build the fork image `trino:481-arm64`: install `trino-main` → rebuild the server tarball → `core/docker/build.sh`. Use `--server` to skip the `trino-main` install when only server/plugin code changed. |
| `restart-trino.sh` | Recreate **only** the `trino` service (new image or changed `dev/trino` config) and wait until it is ready. Leaves MinIO/Lakekeeper/Postgres running. |
| `up.sh` | Bring the whole stack up (`docker compose up -d`). Build the image first. |
| `down.sh` | Stop the stack (`-v` also drops volumes). |
| `run-tests.sh` | Run the CTE-materialization test suite (unit + in-JVM e2e). No running stack required. |
| `trino-cli.sh` | Wrapper around the fork CLI against the stack; passes all args through. |

## Typical loop

```bash
cd flw_dev/docker-compose-dev/scripts

./up.sh                     # first time: start MinIO + Lakekeeper + Trino
# ... edit code in the fork ...
./build-image.sh            # rebuild trino:481-arm64
./restart-trino.sh          # swap the new image into the running stack

# verify the feature is live
./trino-cli.sh --execute "SHOW SESSION LIKE 'cte_materialization%'"
```

## Notes

- **Requirements:** Docker, a JDK 25 (`env.sh` finds the Homebrew one; override with `JAVA_HOME=...`).
- **Endpoints:** Trino `https://localhost:8443` (`admin`/`admin`, self-signed → CLI uses `--insecure`);
  Web UI `http://localhost:8080/ui`; MinIO console `http://localhost:9001`; Lakekeeper `http://localhost:8181`.
- **Why the odd Maven flags** (`-Dair.check.skip-all=true`, `-Dskip.npm=true`, …): the airbase enforcer
  rejects the Homebrew JDK vendor and the web-ui npm step can hang — see comments in `env.sh`.
- **`dev/trino/etc` config** in this stack carries non-default settings used while developing the feature:
  the `lakehouse` catalog has `fs.cache.*` enabled, and `config.properties` enables the orphan sweeper with
  **short test values** (`cte-materialization.orphan-sweep.interval=20s`, `min-age=5s`). For anything other
  than local testing use a `min-age` larger than your longest expected query (see the feature docs).
- **Inspecting scratch tables:** they are dropped asynchronously the instant the query finishes, so
  `SHOW TABLES` rarely catches one. Look at `system.runtime.queries` for the `CREATE TABLE ...cte_<name>_<queryid>`
  child statements (or the Web UI) instead.
