#!/usr/bin/env bash
# Stop and remove the dev stack. Pass -v to also drop volumes (wipes Lakekeeper metadata; the MinIO
# `warehouse` bucket is reset on the next `up` by the `mc` service regardless).
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

dc down "$@"
