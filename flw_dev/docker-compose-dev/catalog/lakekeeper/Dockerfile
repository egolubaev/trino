# Lakekeeper — Apache Iceberg REST catalog.
#
# Version pinned to match production:
#   infra-analytics/apps/lakekeeper/values-prod.yaml
#     -> lakekeeper.catalog.image.tag: v0.12.3
#
# Upstream image is distroless and runs as the non-root "nonroot" user.
# The binary lives at /home/nonroot/lakekeeper and exposes the subcommands
# `migrate`, `serve` and `healthcheck` (the command is set per-service in the
# compose file). Port 8181 serves both the Iceberg REST API (/catalog) and the
# management API (/management).
FROM quay.io/lakekeeper/catalog:v0.12.3
