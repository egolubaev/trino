# Design decisions, limitations, and backlog

## Deliberate limitations

### Non-determinism: hard-excluded (kept as-is)

A CTE is materialized only if it — and everything in its dependency closure — is deterministic. The
denylist is `rand`, `random`, `uuid`, `shuffle`, `now`, `secure_rand`, `secure_random`, plus any AST node
whose class name starts with `Current` (`CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_USER`,
`CURRENT_CATALOG`, …). The check is transitive and memoized.

Why: materialization captures **one** evaluation of the CTE and reuses it for every reference. Inlining
re-evaluates per reference. For a non-deterministic CTE these differ:

```sql
WITH d AS (SELECT rand() AS r), c AS (SELECT r FROM d)
SELECT * FROM c x, c y, d z   -- inlined: x.r, y.r, z.r are 3 different randoms
                              -- materialized: x.r == y.r (one captured value) -> changed semantics
```

Decision: **leave hard-excluded and document it.** We will not add an opt-in to materialize
non-deterministic CTEs. The correctness risk (silently changing results) outweighs the benefit, and there
is no way to know whether a user's query depends on per-reference re-evaluation. This is now stated in the
user docs under *Eligibility and limitations*.

### Column-alias CTEs: inlined (not yet supported)

A CTE written with an explicit column-alias list is currently never materialized:

```sql
WITH cm (a, b) AS (SELECT x + 1, sum(y) FROM t GROUP BY x)   -- gated out: getColumnNames().isPresent()
SELECT a FROM cm c1 JOIN cm c2 ON c1.a = c2.a
```

The gate is `CteMaterializer.findCandidates` → `if (withQuery.getColumnNames().isPresent()) continue;`.

**Why it is a problem.** The alias list `(a, b)` names the CTE's output columns — and people most often
add it precisely because the body's `SELECT` items are *unnamed expressions* (`x + 1`, `sum(y)`). Our
scratch table is built with `CREATE TABLE scratch AS <cte body>`, and the alias list is **not** part of
the body — it lives on the `WithQuery`, outside. So the CTAS would try to materialize
`SELECT x + 1, sum(y) FROM t GROUP BY x`, whose columns have no names, and fail with *"Column name not
specified at position N"*. Even when the body columns happen to be named, relying on the alias list to
rename them positionally is something the current scratch source does not reproduce. Rather than
materialize-then-fail (and silently fall back), the feature excludes these CTEs up front.

Note this is distinct from, and stricter than, the unnamed-column issue in general: a CTE *without* an
alias list but *with* unnamed body columns (`WITH cm AS (SELECT x+1 FROM t) ...`) would also fail the CTAS
and fall back. Real-world CTEs we target almost always name their `SELECT` items (e.g.
`sum(extendedprice) AS rev`), so this has not bitten us — but it is the same root cause: **the scratch
CTAS needs every output column to have a name, and the alias-list case is the common way columns end up
unnamed.**

**How to support it (when we choose to).** Carry the alias list into the scratch CTAS so the materialized
columns get the right names. Two viable forms:

1. Emit `CREATE TABLE scratch (a, b) AS <body>` — Trino's CTAS accepts a target column-alias list, which
   names the columns regardless of the body, and also covers the unnamed-expression case. Smallest change:
   `buildScratchSource` would return the column names alongside the SQL, and the orchestrator's
   `materialize` would interpolate them.
2. Rewrite the body's outermost `SELECT` items to `expr AS alias` before the CTAS.

Edge cases either way: the body being a set operation (`UNION`) or `VALUES` (where "outermost SELECT
items" is not well defined), and an alias count that does not match the body's column count (illegal SQL,
already rejected by analysis). Form 1 is cleaner and handles set operations. Bounded work; deferred.

## Decisions that shaped the current design

- **Staged scratch tables, not in-engine reuse.** Forced by Trino's tree-shaped `SubPlan` (see
  [architecture.md](architecture.md#why-staged-not-in-engine)). The in-engine alternative is M7 below.
- **Off by default, fail-safe always.** `cte_materialization_strategy=NONE` by default; any error in the
  whole path falls back to plain inlining. The feature must never change a result or fail a query.
- **Reference count across the whole statement.** Counting references in sibling CTE bodies too means a
  CTE shared by two materialized siblings is itself materialized once, instead of re-scanned per sibling.
- **Cost gate prunes, never blocks.** Under `HEURISTIC`, unknown statistics ⇒ materialize. The gate only
  declines when it can *prove* the repeated scan is small, so missing stats never silently disable the
  feature.
- **Per-CTE, source-catalog-derived scratch placement.** Each materialized CTE is placed in the **same
  catalog as the data it reads** (its first fully-qualified source table, following the CTE's dependency
  closure), not in the session's default catalog. This is the multi-connector fix: a CTE that reads a
  non-Iceberg catalog (e.g. ClickHouse) must not be materialized into Iceberg. Fallbacks when the CTE has
  no qualified source: session default catalog.schema (the common unqualified case), then any qualified
  source table in the statement. Placement is per-CTE, so a query whose CTEs read different catalogs
  materializes each into its own catalog; a CTE with no derivable location is inlined while others in the
  same query are still materialized. This also fixed the original JDBC/BI footgun (DBeaver connecting
  without a default schema → silent no-op). Caveat unchanged: a read-only source catalog (e.g. `tpch`)
  makes that CTE's CTAS fail and fall back to inlining.
- **Per-catalog scratch-schema override** (`cte-materialization.scratch-schemas`,
  `sourceCatalog:targetCatalog.targetSchema`). The source catalog determined above is redirected to a
  configured target schema when set (e.g. `lakehouse:lakehouse.temp_cte`). This is the
  *dedicated scratch schema*, done per-catalog rather than as one global schema — necessary precisely
  because different source catalogs need their scratch in different connectors. It keeps scratch out of
  data schemas and gives the sweeper one known set of schemas to watch.
- **Internal scratch CTAS/DROP bypass resource-group admission.** The scratch query runs while the parent
  is already admitted and holding a slot in its resource group during planning; routing the child through
  the same group risks a deadlock (child queues at the group's `hardConcurrencyLimit` while the parent
  blocks waiting for it). The child therefore skips group selection/queueing
  (`DispatchManager.createQuery(..., bypassResourceGroupAdmission=true)` → `startWaitingForResources()`
  directly) and starts immediately. Memory accounting and worker scheduling are unchanged, so cluster
  memory limits still apply — only queue admission is skipped. Rationale: if the parent is running, its
  materialization must be able to run too.
- **Orphan sweeper gates on query state, not existence (M6).** A still-running query's scratch is never
  dropped, regardless of age; a finished-but-leaked or unknown query's scratch is reclaimed after
  `min-age`. This keeps long-running (multi-hour) queries safe by *state* rather than relying on a large
  `min-age`, and reclaims listener-failure leaks without waiting for the query to expire from history.
- **fs.cache is safe.** With `fs.cache.enabled=true` the materialized vs inlined results are bit-identical
  (verified cold cache, warm cache, and warm-cache-with-a-new-scratch-UUID). Iceberg gives every table a
  unique UUID location and scratch names embed the query id, so data-file paths are globally unique — the
  cache can never serve a new scratch a previous one's bytes. Only caveat is efficiency: dropped scratch
  bytes linger in the cache until TTL/LRU; bounded by `max-disk-usage`, negligible for aggregating CTEs.

## Backlog

### Done (was previously "not doing")

- **Dedicated scratch schema** — **reversed and implemented** as the per-catalog
  `cte-materialization.scratch-schemas` override (see *Decisions that shaped the current design* above).
  The original "not doing" rationale assumed a single global schema; the multi-connector requirement (a
  ClickHouse-sourced CTE must not land in Iceberg) made a per-catalog mapping necessary, and that same
  mapping cleanly delivers the dedicated-temp-schema benefit (sweeper target + keeping scratch out of data
  schemas). Placement is now: source catalog (per CTE) → per-catalog override → session default.

### Not doing (explicit decisions)

- **Multi-coordinator-correct sweeper.** Would need a cluster-wide query-state registry (no built-in
  mechanism — each coordinator only tracks its own queries). Staying single-coordinator; multi-coordinator
  safety is the operator's `min-age` (> longest expected query). Documented.
- **fs.cache bypass for scratch reads.** The cache-pollution caveat is bounded and minor; not worth a
  per-table cache-bypass mechanism.

### Open (would do if needed)

- **Column-alias CTEs** — see the limitation above; support via CTAS target column list.
- **Resource *attribution* for internal queries.** Admission deadlock is solved (internal queries bypass
  admission entirely). What remains is *accounting*: the scratch CTAS/DROP still run as their own queries,
  so their CPU/memory is not attributed to the parent. Lower priority than the deadlock (which is fixed);
  would attribute child cost back to the parent for monitoring/billing.
- **Lineage of the executed main query reports scratch tables, not sources.** The rewritten query's
  `QueryCompletedEvent` inputs are the `cte_*` scratch tables; the real source reads live in the child
  CTAS query, so event-listener-based lineage splits across the two queries. Not a problem for
  EXPLAIN-based lineage (`EXPLAIN` is not a `Query`, so it never materializes and shows the original
  sources). If needed for event-listener lineage, set the main query's reported inputs from the original
  pre-rewrite analysis. Deferred — current consumers extract lineage via EXPLAIN.
- **Cap + parallelism for many CTEs.** No upper bound on CTEs materialized per query, and the scratch
  CTAS run sequentially/blocking. Could add a max-per-query and run independent CTEs' CTAS in parallel
  (dependency-ordered ones must still serialize). Parallelism is now safe to add since the admission
  deadlock is solved.

### M7: in-engine CteProducer / CteConsumer

The "proper" alternative: instead of staging to scratch tables, add logical plan nodes so a CTE is
computed once **inside one query plan** and its result is reused by all consumers via a shared
exchange/spool — no scratch tables, no child queries.

**Pros**

- No scratch tables at all: no orphan problem (M6 becomes unnecessary), no catalog pollution, no
  DROP/PURGE, no fs.cache pollution.
- One query: unified accounting, resource groups, cancellation, and monitoring; it shows up in
  `EXPLAIN ANALYZE` as a single plan.
- Lower latency: producer feeds consumers through an in-memory/spooled exchange, pipelined — no separate
  query submission and commit round-trips.
- Fully cost-based and per-fragment: the optimizer decides reuse with the real plan and stats.
- Works regardless of connector write capability (no writable catalog needed) and regardless of the
  session's default schema — both M5's read-only-source caveat and the placement logic disappear.
- No CTAS column-naming constraint, so the column-alias and unnamed-column limitations disappear.

**Cons**

- Large, high-risk core-engine change. Trino's `SubPlan` is a strict tree — a fragment's output goes to
  exactly one parent — so feeding one producer's output to multiple consumer branches requires a DAG the
  execution engine does not support today. This is the fundamental blocker; it touches the planner,
  fragmenter, scheduler, exchange layer, and operators.
- Memory/spill: the materialized result must be held for the duration; needs memory management and spill
  integration.
- Fault tolerance: integrating reuse with both pipelined and fault-tolerant (spooling) execution modes is
  complex.
- Maintenance / upstream divergence: this is exactly the kind of feature upstream may implement on top of
  the spooling exchange; a fork implementation is costly to carry and may conflict with that direction.
- Per-query only: no path to cross-query reuse (the scratch approach could in principle be extended to it).

**Verdict.** The staged approach we built is the pragmatic choice: it works on the stock engine with no
core changes and delivers the bulk of the benefit (one scan instead of N). M7 is architecturally cleaner
but is a major engine project gated on removing the tree-shaped-fragment limitation. Revisit only if the
scratch-table model proves limiting in production.
