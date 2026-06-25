# CTE materialization (fork feature)

Automatic materialization of multiply-referenced CTEs into per-query scratch tables, so a CTE's
expensive scan/aggregation runs **once** instead of once per textual reference. Branch `cte_materializing`
(Trino 481).

This directory is the internal engineering record. The user-facing reference lives in the main docs at
[`docs/src/main/sphinx/optimizer/cte-materialization.md`](../../../docs/src/main/sphinx/optimizer/cte-materialization.md).

- [architecture.md](architecture.md) — how it works internally: the staged pipeline, the classes, the
  request flow, and where each piece lives in the tree.
- [design-decisions.md](design-decisions.md) — why it is built this way, the deliberate limitations
  (non-determinism, column aliases), what is **not** done and why, and the in-engine alternative (M7).
- [grafana-dashboard.json](grafana-dashboard.json) — importable Grafana dashboard over the
  `trino.cte:name=CteMaterializationStats` JMX metrics (health / adoption / cost). Grafana → Dashboards →
  New → Import → upload the file, pick the Prometheus datasource. Queries use case-insensitive `__name__`
  regexes (`trino_cte.*…`) so they match regardless of how the JMX exporter renders the MBean name; if no
  series show, confirm the exact names in Prometheus with `{__name__=~"(?i)trino_cte.*"}`.

## The problem

Trino inlines a CTE at every reference — there is no memoization. `RelationPlanner.visitTable` calls
`process(namedQuery)` once per reference, producing N independent subtrees and N table scans. A CTE that
scans a large table and is referenced 8 times scans that table 8 times. This is the "`cm_data` scanned
8×" pattern we hit in the plan/fact star schema work.

## The approach (staged pre-execution)

Detect eligible CTEs → run each as its own `CREATE TABLE scratch AS <cte body>` (committed in its own
transaction) → rewrite the main query so the CTE body becomes `SELECT * FROM scratch` → re-analyze and
plan → drop the scratch tables when the query terminates. Off by default; enabled via session properties.
In-engine plan-node reuse (a `CteProducer`/`CteConsumer` DAG) was rejected for now — see
[design-decisions.md](design-decisions.md#m7-in-engine-cteproducer--cteconsumer) for why.

## Measured effect (live, Lakekeeper + MinIO)

Source `cte_bench.src` = 36,007,290 rows; CTE aggregates `GROUP BY suppkey` and is referenced 8×.

| | wall-clock | CPU | rows scanned by the main query |
| --- | --- | --- | --- |
| inlined (`NONE`) | 6.48 s | 34.42 s | 288,058,320 (= 8 × 36M) |
| materialized (`ALL`) | 1.99 s | 44 ms + 2.05 s CTAS | 80,000 (reads scratch only) |

Source scanned 8× → 1×; ~3.3× faster wall-clock, ~16× less CPU. Results are bit-identical for exact types;
`double` sums differ in the last 1–2 ULP because summation order differs across the two plans (not a bug).

## Milestones (all committed on `my-trino-481`)

| | commit | summary |
| --- | --- | --- |
| M1 | `f19ecb645e8` | auto-materialize multiply-referenced CTEs to Iceberg scratch tables |
| M2 | `d6922664f04` | strategy gating: `cte_materialization_strategy` NONE/ALL/HEURISTIC + `min_references` |
| M3 | `066aba16c76` | multi-CTE dependency support (transitive determinism, dependency-ordered scratch) |
| M4 | `00a64f0c429` | stats-based cost gate for HEURISTIC (`min_scan_savings`) |
| M5 | `88f9d48ad52` | place scratch by source table when the session has no default schema |
| M6 | `8583ba7728f` | orphan scratch-table sweeper (gates on query state) |

Test coverage: `TestCteMaterializer` (unit, pure-AST), `TestCteScratchSweeper` (unit, sweeper decision
logic), `TestCteMaterializationEndToEnd` + `TestCteMaterializationOrchestrator` (in-JVM Iceberg REST
catalog). Run them with [`../docker-compose-dev/scripts/run-tests.sh`](../../docker-compose-dev/scripts/run-tests.sh).

## Building / running

See [`../docker-compose-dev/scripts/README.md`](../../docker-compose-dev/scripts/README.md):
`build-image.sh` → `restart-trino.sh`, then `trino-cli.sh`. The dev stack's `dev/trino` config enables the
feature's fs.cache and orphan-sweeper settings with short test values.
