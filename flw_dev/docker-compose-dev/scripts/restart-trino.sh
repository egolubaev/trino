#!/usr/bin/env bash
# Recreate ONLY the trino service (picks up a freshly built image / changed dev/trino config),
# leaving MinIO + Lakekeeper + Postgres untouched, then wait until the server is ready.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

echo "==> recreating trino service"
dc up -d --no-deps --force-recreate trino

echo "==> waiting for Trino to start"
for _ in $(seq 1 60); do
  if curl -sk -u "$TRINO_USER:$TRINO_PASSWORD" "$TRINO_HTTPS_URL/v1/info" 2>/dev/null | grep -q '"starting":false'; then
    echo "==> ready: $TRINO_HTTPS_URL  (Web UI: $TRINO_HTTP_UI, $TRINO_USER/$TRINO_PASSWORD)"
    exit 0
  fi
  sleep 2
done

echo "!! Trino did not become ready; recent logs:" >&2
docker logs trino 2>&1 | tail -40 >&2
exit 1
