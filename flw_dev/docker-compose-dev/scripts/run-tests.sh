#!/usr/bin/env bash
# Run the CTE-materialization test suite (no running stack needed -- these are in-JVM tests).
#   unit (trino-main):     TestCteMaterializer, TestCteScratchSweeper
#   e2e  (trino-iceberg):  TestCteMaterializationEndToEnd, TestCteMaterializationOrchestrator
#                          (spin up an in-JVM Iceberg REST catalog; require trino-main installed to ~/.m2)
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

cd "$FORK_ROOT"

echo "==> unit tests (core/trino-main)"
./mvnw -pl core/trino-main test "${MVN_FLAGS[@]}" -Dtest='TestCteMaterializer,TestCteScratchSweeper'

echo "==> installing trino-main so trino-iceberg tests compile against current sources"
./mvnw -pl core/trino-main install -DskipTests "${MVN_FLAGS[@]}"

echo "==> e2e tests (plugin/trino-iceberg)"
./mvnw -pl plugin/trino-iceberg test "${MVN_FLAGS[@]}" -Dtest='TestCteMaterializationEndToEnd,TestCteMaterializationOrchestrator'

echo "==> all CTE tests passed"
