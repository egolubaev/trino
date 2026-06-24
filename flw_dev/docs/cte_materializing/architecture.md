# Architecture

## Where it hooks in

Materialization runs at plan time, between analysis and logical planning, inside the query execution:

```
SqlQueryExecution.doPlanQuery()
  └─ maybeMaterializeCtes()         <-- the whole feature
       ├─ gate on session strategy + (re)select candidates
       ├─ for each chosen CTE: build scratch SQL, run scratch CTAS, record cleanup
       ├─ rewrite the statement (CTE body -> SELECT * FROM scratch)
       └─ re-analyze the rewritten statement -> Analysis used for planning
  └─ LogicalPlanner.plan(analysis)  <-- plans the rewritten (or original, on fallback) statement
```

`maybeMaterializeCtes()` returns the `Analysis` to plan: the rewritten one on success, or the original
(inlining) on any failure. It is wrapped so the feature can never break a query.

File: `core/trino-main/src/main/java/io/trino/execution/SqlQueryExecution.java`
(`maybeMaterializeCtes`, `selectCandidates`, `estimateSourceRows`, `scratchSchemaPrefix`,
`scratchTableName`, `registerScratchCleanup`).

## Request flow

```
                          ┌─────────────────────────────────────────────┐
   user query (Analysis)  │ maybeMaterializeCtes (SqlQueryExecution)     │
 ────────────────────────▶│                                             │
                          │ 1. strategy == NONE ? ───────────────▶ inline│
                          │ 2. CteMaterializer.findCandidates(stmt)      │  pure AST: eligibility gates
                          │ 3. selectCandidates(strategy)                │  ALL: keep all
                          │      └─ HEURISTIC: refs >= min_references     │  HEURISTIC: + cost gate
                          │         and (refs-1)*sourceRows >= savings    │      (estimateSourceRows: Metadata stats)
                          │ 4. scratchSchemaPrefix(session, stmt)        │  session default, else source table
                          │ 5. for each candidate (WITH order):          │
                          │      buildScratchSource() ───────────────────┼──▶ CteMaterializationOrchestrator
                          │      orchestrator.materialize(scratch, sql)  │      DirectTrinoClient: CREATE TABLE AS
                          │      registerScratchCleanup(scratch)         │      (autocommit child txn)
                          │ 6. CteMaterializer.rewrite(stmt, scratches)  │  CTE body -> SELECT * FROM scratch
                          │ 7. analyzerFactory.analyze(rewritten)        │
                          └───────────────────────────┬─────────────────┘
                                                       ▼
                                              Analysis (rewritten)  ──▶ LogicalPlanner

   on query terminal state ──▶ cleanup listener ──▶ orchestrator.cleanupAsync(scratch)  (DROP ... purge)
   periodically (coordinator) ─▶ CteScratchSweeper ─▶ orchestrator.cleanup(orphan)      (leaked scratch)
```

## Components

### Detection and rewrite — `io.trino.cte.CteMaterializer` (pure AST, no catalog)

- `findCandidates(Statement)` → `List<CteCandidate>` where `CteCandidate(name, referenceCount)`.
  Applies the eligibility gates: non-recursive `WITH`, no cycle, transitive determinism, no column-alias
  list, no nested-`WITH`-with-dependencies, and reference count ≥ 2 counted across the **whole** statement
  (main query + sibling CTE bodies, so a CTE shared by two materialized siblings counts as ≥ 2).
- `buildScratchSource(stmt, cteName, nameToScratch, parser)` → the `AS`-source for one scratch `CREATE
  TABLE`. Dependencies already in `nameToScratch` are redefined as `SELECT * FROM <their scratch>`; the
  rest are inlined via a `WITH` clause over the CTE's transitive dependency closure. An independent CTE
  yields just its body.
- `rewrite(stmt, nameToScratch, parser)` → swaps each materialized CTE's body for `SELECT * FROM <scratch>`,
  leaving non-materialized CTEs untouched. References resolve on re-analysis; per-reference pushdown still
  specializes.
- `sourceTablesForCostEstimate(stmt, cteName)` → base tables a standalone CTE scans (empty when it
  depends on a sibling — size then unknown), used by the cost gate.
- `firstQualifiedTable(stmt)` → first `catalog.schema.table`, used for scratch placement fallback.

Determinism is computed transitively and memoized (a CTE is non-deterministic if it, or anything in its
dependency closure, uses a non-deterministic function or `CURRENT_*` value). The dependency graph is
built from unqualified table references that match sibling CTE names; a cycle disables the whole statement
defensively.

### Running internal statements — `io.trino.cte.CteMaterializationOrchestrator`

Wraps `io.trino.client.direct.DirectTrinoClient` — the in-process twin of the `/v1/statement` submit path
— to run the scratch `CREATE TABLE ... AS` and the `DROP TABLE IF EXISTS` as their own queries.

- Each internal statement runs in a fresh **autocommit** transaction via
  `SessionContext.fromSessionWithoutTransaction(parentSession)`, so the scratch CTAS commits independently
  and is immediately visible to the (still in-flight) parent query. This "one query spawns and awaits a
  child query" shape has no in-tree precedent and was the main thing the spike de-risked.
- The child session sets `cte_materialization_strategy=NONE` so materialization does not recurse.
- `DispatchManager` and `QueryManager` are injected as `Provider`s and `DirectTrinoClient` is built lazily,
  to break the Guice construction cycle (DispatchManager → query-execution factory → orchestrator).
- `cleanupAsync` drops on a background executor (used from the terminal-state listener so the DROP never
  stalls the thread firing the state transition).

### Strategy and cost gate — in `SqlQueryExecution`

- `selectCandidates` applies the strategy: `ALL` keeps every eligible candidate; `HEURISTIC` keeps those
  with `referenceCount >= cte_materialization_min_references` and estimated savings
  `(referenceCount - 1) * sourceRows >= cte_materialization_min_scan_savings`.
- `estimateSourceRows` resolves the CTE's base tables with `plannerContext.getMetadata().getTableHandle`
  and sums `getTableStatistics(...).getRowCount()`. Unknown stats / unresolved table / CTE-with-deps ⇒
  unknown ⇒ keep (the gate only ever *prunes* when it can prove the scan is small). Runs pre-plan, so it
  uses table statistics directly rather than a `PlanNodeStatsEstimate`.

### Scratch placement — `scratchSchemaPrefix` in `SqlQueryExecution`

`session catalog.schema` if both are set, else the catalog.schema of `firstQualifiedTable(stmt)`, else
inline. Scratch name is `<catalog>.<schema>.cte_<sanitized-name>_<queryId>`.

### Cleanup and the orphan sweeper

- Per-query: `registerScratchCleanup` adds a state-change listener that calls `cleanupAsync` once the
  query is done.
- Crash/failure leaks: `io.trino.cte.CteScratchSweeper` (coordinator singleton, `@PostConstruct`-scheduled)
  periodically lists `cte_*` tables in the configured schemas (via `TransactionBuilder` +
  `Metadata.listTables` on an internal session) and drops those whose query is not currently executing and
  whose query-id timestamp is older than `min-age`. Decision logic lives in the pure static helpers
  `extractQueryId` / `queryIdTimestamp` / `isSweepableOrphan`. The query-id timestamp is parsed as UTC
  because `QueryIdGenerator` stamps it in UTC.

### Configuration — `io.trino.cte.CteMaterializationConfig`

`cte-materialization.orphan-sweep.schemas|interval|min-age`, bound in `CoordinatorModule`. The
materialization session properties (`cte_materialization_strategy`, `…_min_references`,
`…_min_scan_savings`) are registered in `SystemSessionProperties`.

## File map

```
core/trino-main/src/main/java/io/trino/cte/
  CteMaterializer.java                 detection + rewrite + scratch-source construction (pure AST)
  CteMaterializationOrchestrator.java  runs scratch CREATE/DROP via DirectTrinoClient (autocommit child txn)
  CteMaterializationStrategy.java      NONE / ALL / HEURISTIC
  CteMaterializationConfig.java        orphan-sweep config
  CteScratchSweeper.java               background reclaim of leaked scratch tables
core/trino-main/src/main/java/io/trino/
  execution/SqlQueryExecution.java     maybeMaterializeCtes orchestration + strategy/cost gate + placement + cleanup
  server/SessionContext.java           fromSessionWithoutTransaction (autocommit child session)
  SystemSessionProperties.java         the three session properties
  server/CoordinatorModule.java        binds orchestrator + config + sweeper
core/trino-main/src/test/java/io/trino/cte/
  TestCteMaterializer.java             pure-AST detection/rewrite/cost-source/placement helpers
  TestCteScratchSweeper.java           sweeper decision logic
plugin/trino-iceberg/src/test/java/io/trino/plugin/iceberg/cte/
  TestCteMaterializationOrchestrator.java   internal CTAS commits + is visible to a fresh query
  TestCteMaterializationEndToEnd.java        real WITH auto-materializes; strategy/cost/placement/chain
```

## Why staged, not in-engine

Trino's `SubPlan` is a strict tree: a fragment's output goes to exactly one parent, so a single producer
cannot feed several consumer branches within one plan. Exchange/spool reuse and "write-then-read a
connector table in one plan" are therefore not available (a `TableFinishNode` emits only a row count, with
no commit→scan ordering edge). Staging each CTE as its own committed query side-steps all of this on the
stock engine. The trade-off and the in-engine alternative are in
[design-decisions.md](design-decisions.md#m7-in-engine-cteproducer--cteconsumer).
