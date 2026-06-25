/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.cte;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.log.Logger;
import jakarta.annotation.PreDestroy;
import io.trino.Session;
import io.trino.client.direct.DirectTrinoClient;
import io.trino.client.direct.DirectTrinoClient.QueryResultsListener;
import io.trino.dispatcher.DispatchManager;
import io.trino.dispatcher.DispatchQuery;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryManagerConfig;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.server.SessionContext;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.type.Type;
import org.intellij.lang.annotations.Language;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.QueryState.FAILED;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

/**
 * Runs internal statements (CTE-materialization scratch CTAS and its DROP) on the coordinator by
 * reusing {@link DirectTrinoClient} — the production in-process twin of the /v1/statement submit path.
 * <p>
 * Each internal statement runs as its own query in a fresh autocommit transaction (see
 * {@link SessionContext#fromSessionWithoutTransaction}), so a scratch CTAS commits independently and
 * is visible to the originating query. The child also has CTE materialization disabled to prevent
 * recursive materialization.
 * <p>
 * {@link DispatchManager}/{@link QueryManager} are injected as {@link Provider}s to break the Guice
 * construction cycle (DispatchManager -&gt; query-execution factory -&gt; this), and the
 * {@link DirectTrinoClient} is built lazily on first use.
 */
public class CteMaterializationOrchestrator
{
    private static final QueryResultsListener DISCARD_RESULTS = new QueryResultsListener()
    {
        @Override
        public void setOutputColumns(List<String> columnNames, List<Type> columnTypes) {}

        @Override
        public void consumeOutputPage(Page page) {}
    };

    private static final Logger log = Logger.get(CteMaterializationOrchestrator.class);

    private final Provider<DispatchManager> dispatchManagerProvider;
    private final Provider<QueryManager> queryManagerProvider;
    private final QueryManagerConfig queryManagerConfig;
    private final DirectExchangeClientSupplier directExchangeClientSupplier;
    private final BlockEncodingSerde blockEncodingSerde;
    private final Map<String, String> scratchSchemaOverrides;
    private final CteMaterializationStats stats;
    private final ExecutorService cleanupExecutor = newCachedThreadPool(daemonThreadsNamed("cte-scratch-cleanup-%s"));
    private final ExecutorService materializationExecutor = newCachedThreadPool(daemonThreadsNamed("cte-materialize-%s"));

    private volatile DirectTrinoClient directTrinoClient;

    @Inject
    public CteMaterializationOrchestrator(
            Provider<DispatchManager> dispatchManagerProvider,
            Provider<QueryManager> queryManagerProvider,
            QueryManagerConfig queryManagerConfig,
            DirectExchangeClientSupplier directExchangeClientSupplier,
            BlockEncodingSerde blockEncodingSerde,
            CteMaterializationConfig cteMaterializationConfig,
            CteMaterializationStats stats)
    {
        this.dispatchManagerProvider = requireNonNull(dispatchManagerProvider, "dispatchManagerProvider is null");
        this.queryManagerProvider = requireNonNull(queryManagerProvider, "queryManagerProvider is null");
        this.queryManagerConfig = requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.directExchangeClientSupplier = requireNonNull(directExchangeClientSupplier, "directExchangeClientSupplier is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.scratchSchemaOverrides = requireNonNull(cteMaterializationConfig, "cteMaterializationConfig is null").scratchSchemaOverrides();
        this.stats = requireNonNull(stats, "stats is null");
    }

    public CteMaterializationStats stats()
    {
        return stats;
    }

    /**
     * Configured {@code targetCatalog.targetSchema} in which to materialize a CTE whose source data lives in
     * {@code sourceCatalog}, if any (from {@code cte-materialization.scratch-schemas}). Empty means place the
     * scratch table in the source table's own catalog/schema.
     */
    public Optional<String> scratchSchemaOverride(String sourceCatalog)
    {
        return Optional.ofNullable(scratchSchemaOverrides.get(sourceCatalog.toLowerCase(ENGLISH)));
    }

    @PreDestroy
    public void shutdown()
    {
        materializationExecutor.shutdownNow();
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private DirectTrinoClient client()
    {
        DirectTrinoClient client = directTrinoClient;
        if (client == null) {
            synchronized (this) {
                client = directTrinoClient;
                if (client == null) {
                    client = new DirectTrinoClient(
                            dispatchManagerProvider.get(),
                            queryManagerProvider.get(),
                            queryManagerConfig,
                            directExchangeClientSupplier,
                            blockEncodingSerde,
                            // bypass resource-group admission: a scratch CTAS runs while its parent query
                            // is already admitted, so routing it through the parent's group could deadlock
                            true);
                    directTrinoClient = client;
                }
            }
        }
        return client;
    }

    /**
     * Materialize {@code cteBodySql} into {@code scratchTable} (a fully-qualified catalog.schema.table)
     * as an internal {@code CREATE TABLE ... AS} in its own autocommit transaction. Blocks until the
     * scratch table is committed. Throws if the CTAS fails.
     */
    public QueryId materialize(Session parentSession, String scratchTable, @Language("SQL") String cteBodySql)
    {
        requireNonNull(scratchTable, "scratchTable is null");
        requireNonNull(cteBodySql, "cteBodySql is null");
        long start = System.nanoTime();
        QueryId queryId = run(parentSession, "CREATE TABLE " + scratchTable + " AS " + cteBodySql);
        stats.scratchCreated(System.nanoTime() - start);
        return queryId;
    }

    /**
     * Materialize a batch of <b>independent</b> scratch tables (no inter-dependencies within the batch),
     * running up to {@code maxConcurrent} CTAS at once. Blocks until all commit. If any CTAS fails, the
     * others still settle (their committed tables are registered for the caller's terminal cleanup) and the
     * first failure is rethrown so the caller can fall back to inlining. With a single table or
     * {@code maxConcurrent <= 1} this is a plain sequential loop.
     */
    public void materializeLevel(Session parentSession, List<String> scratchTables, List<String> cteBodySqls, int maxConcurrent)
    {
        checkArgument(scratchTables.size() == cteBodySqls.size(), "scratchTables and cteBodySqls differ in size");
        if (scratchTables.size() <= 1 || maxConcurrent <= 1) {
            for (int i = 0; i < scratchTables.size(); i++) {
                materialize(parentSession, scratchTables.get(i), cteBodySqls.get(i));
            }
            return;
        }
        Semaphore permits = new Semaphore(maxConcurrent);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < scratchTables.size(); i++) {
            String scratchTable = scratchTables.get(i);
            String cteBodySql = cteBodySqls.get(i);
            futures.add(materializationExecutor.submit(() -> {
                permits.acquireUninterruptibly();
                try {
                    materialize(parentSession, scratchTable, cteBodySql);
                }
                finally {
                    permits.release();
                }
            }));
        }
        awaitAll(futures);
    }

    private static void awaitAll(List<Future<?>> futures)
    {
        RuntimeException failure = null;
        for (Future<?> future : futures) {
            try {
                future.get();
            }
            catch (ExecutionException e) {
                Throwable cause = e.getCause();
                RuntimeException asRuntime = (cause instanceof RuntimeException re) ? re : new RuntimeException(cause);
                if (failure == null) {
                    failure = asRuntime;
                }
                else {
                    failure.addSuppressed(asRuntime);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (failure == null) {
                    failure = new RuntimeException("Interrupted while materializing CTEs", e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Drop a scratch table created by {@link #materialize}. Under the Iceberg REST catalog this DROP
     * purges (catalog deletes data files). Idempotent via IF EXISTS.
     */
    public void cleanup(Session parentSession, String scratchTable)
    {
        requireNonNull(scratchTable, "scratchTable is null");
        run(parentSession, "DROP TABLE IF EXISTS " + scratchTable);
    }

    /**
     * Drop a scratch table on a background thread. Used from query state-change listeners so the
     * (blocking) DROP never stalls the thread firing the terminal transition.
     */
    public void cleanupAsync(Session parentSession, String scratchTable)
    {
        requireNonNull(scratchTable, "scratchTable is null");
        cleanupExecutor.execute(() -> {
            try {
                cleanup(parentSession, scratchTable);
                stats.scratchDropped();
            }
            catch (RuntimeException e) {
                stats.scratchDropFailed();
                log.warn(e, "Failed to drop CTE scratch table %s", scratchTable);
            }
        });
    }

    private QueryId run(Session parentSession, @Language("SQL") String sql)
    {
        // Fresh autocommit transaction. We deliberately do NOT set cte_materialization_strategy=NONE on the
        // child: the internal statements are CREATE TABLE ... AS / DROP TABLE, which are not Query statements,
        // so CteMaterializer.findCandidates never matches them and materialization cannot recurse. Setting a
        // system session property here would instead be validated against the *end user's* permissions at
        // transaction begin (e.g. OPA's checkCanSetSystemSessionProperty), failing the internal CTAS/DROP for
        // any user not allowed to set that property.
        SessionContext context = SessionContext.fromSessionWithoutTransaction(parentSession);
        DispatchQuery query = client().execute(context, sql, DISCARD_RESULTS);
        QueryInfo info = query.getFullQueryInfo();
        if (info.getState() == FAILED) {
            throw info.getFailureInfo().toException();
        }
        return query.getQueryId();
    }
}
