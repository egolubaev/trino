#!/usr/bin/env bash
# Common environment + paths for the dev-stack helper scripts.
# Sourced by the other scripts in this directory; not meant to be run directly.
#
# Layout (relative to this file):
#   scripts/                 <- here
#   ../                      <- docker-compose-dev (COMPOSE_DIR: docker-compose.yml + dev/ config)
#   ../../../                <- trino fork root (FORK_ROOT: ./mvnw, core/docker/build.sh, ...)

set -euo pipefail

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$(cd "$SCRIPTS_DIR/.." && pwd)"
FORK_ROOT="$(cd "$SCRIPTS_DIR/../../.." && pwd)"
COMPOSE_FILE="$COMPOSE_DIR/docker-compose.yml"

# JDK 25 (Homebrew). Override by exporting JAVA_HOME before calling.
export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 25 2>/dev/null || echo /opt/homebrew/Cellar/openjdk/25.0.1/libexec/openjdk.jdk/Contents/Home)}"

# Maven flags that make the fork build on this machine:
#   air.check.skip-all      airbase enforcer rejects the Homebrew JDK vendor
#   skip.npm/installnodenpm skip the trino-web-ui frontend build (npm step can hang)
#   retryHandler.count      Maven Central is reachable but occasionally drops the connection
MVN_FLAGS=(-B -Dair.check.skip-all=true -Dskip.npm=true -Dskip.installnodenpm=true -Daether.connector.http.retryHandler.count=10)

# Image / arch produced by core/docker/build.sh
ARCH="${ARCH:-arm64}"
IMAGE_TAG="${IMAGE_TAG:-trino:481-${ARCH}}"

# Trino endpoints / credentials (dev stack: PASSWORD auth, self-signed TLS)
export TRINO_USER="${TRINO_USER:-admin}"
export TRINO_PASSWORD="${TRINO_PASSWORD:-admin}"
TRINO_HTTPS_URL="${TRINO_HTTPS_URL:-https://localhost:8443}"
TRINO_HTTP_UI="${TRINO_HTTP_UI:-http://localhost:8080/ui}"
CLI_JAR="$FORK_ROOT/client/trino-cli/target/trino-cli-481-executable.jar"

dc() { docker compose --project-directory "$COMPOSE_DIR" -f "$COMPOSE_FILE" "$@"; }
