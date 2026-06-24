#!/usr/bin/env bash
# Bring the whole dev stack up (MinIO + mc + Postgres + Lakekeeper migrate/serve/bootstrap/warehouse + Trino).
# Bring-up order is enforced by the healthchecks / completion conditions in docker-compose.yml.
# NOTE: the `trino` service uses the prebuilt image $IMAGE_TAG -- run ./build-image.sh first if you
# have not built it yet.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

dc up -d "$@"
echo "==> stack starting. Trino: $TRINO_HTTPS_URL (Web UI $TRINO_HTTP_UI, $TRINO_USER/$TRINO_PASSWORD)"
echo "    MinIO console: http://localhost:9001 (admin/password)   Lakekeeper: http://localhost:8181"
