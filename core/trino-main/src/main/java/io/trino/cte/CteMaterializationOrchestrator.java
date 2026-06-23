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

import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static io.trino.execution.QueryState.FAILED;
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
    private final ExecutorService cleanupExecutor = newCachedThreadPool(daemonThreadsNamed("cte-scratch-cleanup-%s"));

    private volatile DirectTrinoClient directTrinoClient;

    @Inject
    public CteMaterializationOrchestrator(
            Provider<DispatchManager> dispatchManagerProvider,
            Provider<QueryManager> queryManagerProvider,
            QueryManagerConfig queryManagerConfig,
            DirectExchangeClientSupplier directExchangeClientSupplier,
            BlockEncodingSerde blockEncodingSerde)
    {
        this.dispatchManagerProvider = requireNonNull(dispatchManagerProvider, "dispatchManagerProvider is null");
        this.queryManagerProvider = requireNonNull(queryManagerProvider, "queryManagerProvider is null");
        this.queryManagerConfig = requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.directExchangeClientSupplier = requireNonNull(directExchangeClientSupplier, "directExchangeClientSupplier is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
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
                            blockEncodingSerde);
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
        return run(parentSession, "CREATE TABLE " + scratchTable + " AS " + cteBodySql);
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
            }
            catch (RuntimeException e) {
                log.warn(e, "Failed to drop CTE scratch table %s", scratchTable);
            }
        });
    }

    private QueryId run(Session parentSession, @Language("SQL") String sql)
    {
        // run in a fresh autocommit transaction, with CTE materialization disabled to avoid recursion
        SessionContext context = SessionContext.fromSessionWithoutTransaction(parentSession)
                .withSystemProperty(CTE_MATERIALIZATION_STRATEGY, CteMaterializationStrategy.NONE.name());
        DispatchQuery query = client().execute(context, sql, DISCARD_RESULTS);
        QueryInfo info = query.getFullQueryInfo();
        if (info.getState() == FAILED) {
            throw info.getFailureInfo().toException();
        }
        return query.getQueryId();
    }
}
