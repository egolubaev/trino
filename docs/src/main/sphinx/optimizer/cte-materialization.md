# CTE materialization

A common table expression (CTE) that is referenced more than once in a query is, by default, inlined
at every reference. Each reference becomes an independent subtree of the plan, so a CTE that scans a
large table N times scans that table N times:

```sql
WITH cm AS (
    SELECT suppkey, sum(extendedprice) AS rev, count(*) AS cnt
    FROM orders_wide
    GROUP BY suppkey
)
SELECT (SELECT sum(rev) FROM cm),
       (SELECT max(rev) FROM cm),
       (SELECT sum(cnt) FROM cm),
       (SELECT max(cnt) FROM cm)        -- cm is read 4x -> orders_wide is scanned 4x
```

CTE materialization computes such a CTE **once**, stores the result in a temporary (scratch) table, and
rewrites every reference to read that table instead. The expensive scan and aggregation run a single
time. The feature is off by default and is enabled per query (or per cluster default) with the session
properties below.

It is implemented for the Iceberg connector: each materialized CTE is written as an `CREATE TABLE ... AS`
into a scratch Iceberg table that is committed in its own transaction, read by the rewritten main query,
and dropped when the query terminates (the drop purges its data files through the REST catalog).

## How it works

For an eligible CTE, before the main query is planned:

1. A scratch table named `cte_<cte-name>_<query-id>` is created with `CREATE TABLE ... AS <cte body>`,
   committed in its own autocommit transaction so it is immediately visible.
2. The statement is rewritten so the CTE body becomes `SELECT * FROM <scratch table>` and re-analyzed.
   Predicate and projection pushdown still specialize per reference.
3. When the query reaches a terminal state, the scratch table is dropped asynchronously.

If anything fails along the way, the query transparently falls back to normal inlining — materialization
never changes a query's result or causes it to fail.

CTEs that reference earlier CTEs are supported: dependencies are materialized first (in declaration
order) and the dependent CTE's scratch query reads their scratch tables; dependencies that were not
themselves materialized are inlined into the dependent CTE's scratch query.

## Session properties

### `cte_materialization_strategy`

- **Type:** {ref}`prop-type-string`
- **Allowed values:** `NONE`, `ALL`, `HEURISTIC`
- **Default value:** `NONE`

Controls when an eligible, multiply-referenced CTE is materialized.

- `NONE` — never materialize; CTEs are inlined (stock behavior).
- `ALL` — materialize every eligible CTE.
- `HEURISTIC` — materialize an eligible CTE only when it is referenced at least
  `cte_materialization_min_references` times **and** its estimated repeated-scan savings reach
  `cte_materialization_min_scan_savings` (see below).

### `cte_materialization_min_references`

- **Type:** {ref}`prop-type-integer`
- **Minimum value:** `2`
- **Default value:** `2`

Under `HEURISTIC`, the minimum number of references a CTE must have before it is materialized.

### `cte_materialization_min_scan_savings`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `1000000`

Under `HEURISTIC`, the minimum estimated repeated-scan savings, in rows, before a CTE is materialized.
The estimate is `(references - 1) * source-rows`, where `source-rows` is the sum of the row counts of
the base tables the CTE scans, taken from table statistics. When statistics are unavailable, a table
cannot be resolved, or the CTE depends on another CTE, the savings are treated as unknown and the CTE
is materialized — the heuristic only ever *declines* to materialize when it can show the repeated scan
is small.

## Configuration properties

These cluster-level properties control the background sweeper that reclaims scratch tables leaked by a
coordinator crash or a failed cleanup. They are unrelated to whether materialization happens.

### `cte-materialization.orphan-sweep.schemas`

- **Type:** {ref}`prop-type-string`
- **Default value:** (empty)

Comma-separated list of `catalog.schema` locations to periodically sweep for leaked scratch tables.
Empty (the default) disables the sweeper.

### `cte-materialization.orphan-sweep.interval`

- **Type:** {ref}`prop-type-duration`
- **Default value:** `10m`

How often the sweeper runs.

### `cte-materialization.orphan-sweep.min-age`

- **Type:** {ref}`prop-type-duration`
- **Default value:** `1h`

A leftover scratch table is only dropped when the originating query is not currently executing **and**
the query-id timestamp is older than this value. This must exceed the longest expected query duration;
see [Orphan scratch sweeper](cte-materialization-sweeper) below.

## Scratch table placement

Scratch tables are created in:

1. the session's default catalog and schema, when both are set; otherwise
2. the catalog and schema of the first fully-qualified (`catalog.schema.table`) source table the
   statement reads.

If neither can be determined (no default schema and no fully-qualified source table), the query is
inlined. The target catalog must be writable; if it is read-only (for example a TPC-H catalog used as a
source) the scratch `CREATE TABLE` fails and the query falls back to inlining.

## Eligibility and limitations

A CTE is materialized only when all of the following hold; otherwise it is inlined.

- The statement is a query with a non-recursive `WITH` clause and no dependency cycle among CTEs.
- The CTE is referenced at least twice across the whole statement (the main query plus sibling CTE bodies).
- The CTE, and every CTE in its dependency closure, is **deterministic**. CTEs that use non-deterministic
  functions (`rand`, `random`, `uuid`, `shuffle`, `now`, `secure_random`) or contextual values
  (`CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_USER`, …) are never materialized, because reusing a
  single captured evaluation across references would change results.
- The CTE has **no explicit column-alias list** (`WITH cte (a, b) AS ...`). Such CTEs are currently
  inlined.
- A CTE that depends on a sibling CTE must not also contain its own nested `WITH`.

Materialization only fires for top-level `SELECT` statements. It does not apply to `EXPLAIN` /
`EXPLAIN ANALYZE` (their statement type is not a query), so an `EXPLAIN ANALYZE` of a query shows the
inlined plan even with the feature enabled — compare the two runs in the Web UI instead.

(cte-materialization-sweeper)=
## Orphan scratch sweeper

A materialized CTE's scratch table is normally dropped when its query terminates. If the coordinator
crashes, or the cleanup otherwise fails, the table is leaked. When
`cte-materialization.orphan-sweep.schemas` is configured, a background task periodically lists `cte_*`
tables in those schemas and drops the leaked ones.

A table is dropped only when its originating query is **not currently executing** and its query-id
timestamp is older than `cte-materialization.orphan-sweep.min-age`:

- A still-running query's scratch is never dropped, regardless of how long the query runs.
- A finished query whose cleanup leaked, or a query unknown to the coordinator (for example after a
  restart), is reclaimed once older than `min-age`.

The sweeper assumes a single coordinator: a query running on another coordinator is not observable, so in
a multi-coordinator deployment `min-age` must be larger than the longest expected query duration to avoid
dropping a scratch table that another coordinator is still using.

## Observing materialization

Scratch tables are dropped within moments of the query finishing, so `SHOW TABLES` rarely shows one.
To confirm that materialization happened, look in `system.runtime.queries` for the internal
`CREATE TABLE ... cte_<name>_<query-id>` statements, or open the query in the Web UI. A materialized main
query reads only the small scratch tables, so its input row count is far lower than the inlined run.
