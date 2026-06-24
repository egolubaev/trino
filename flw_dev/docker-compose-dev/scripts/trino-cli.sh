#!/usr/bin/env bash
# Thin wrapper around the fork's trino-cli against the dev stack (PASSWORD auth, self-signed TLS).
# All arguments are passed through to the CLI.
#
#   ./trino-cli.sh --execute "SHOW SESSION LIKE 'cte_materialization%'"
#   ./trino-cli.sh --catalog lakehouse --schema cte_bench --session cte_materialization_strategy=ALL \
#                  --execute "WITH cm AS (...) SELECT ..."
#   ./trino-cli.sh        # interactive shell
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

if [[ ! -f "$CLI_JAR" ]]; then
  echo "CLI jar not found: $CLI_JAR" >&2
  echo "Build it with:  (cd \"$FORK_ROOT\" && ./mvnw -pl client/trino-cli -am package ${MVN_FLAGS[*]} -DskipTests)" >&2
  exit 1
fi

exec java -jar "$CLI_JAR" --server "$TRINO_HTTPS_URL" --user "$TRINO_USER" --password --insecure "$@"
