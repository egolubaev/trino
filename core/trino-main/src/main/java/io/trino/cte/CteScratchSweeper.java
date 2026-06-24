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

import com.google.common.base.Splitter;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryState;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.TransactionBuilder;
import io.trino.transaction.TransactionManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Periodically drops CTE-materialization scratch tables that were leaked when the normal terminal-state
 * cleanup listener never ran (coordinator crash) or failed. For each {@code catalog.schema} in
 * {@code cte-materialization.orphan-sweep.schemas}, lists {@code cte_*} tables and drops those whose
 * originating query is NOT currently executing AND whose query-id timestamp is older than
 * {@code cte-materialization.orphan-sweep.min-age}. A still-running query's scratch is never dropped, no
 * matter how long the query runs. Disabled (no-op) when no schemas are configured.
 * <p>
 * Single-coordinator deployments are fully safe: a running query is always observable via
 * {@link QueryManager} here. In a multi-coordinator deployment a query running on another coordinator is
 * unknown here, so the min-age guard (which should exceed the longest expected query duration) is the
 * remaining safeguard against dropping a live scratch table.
 */
public class CteScratchSweeper
{
    private static final Logger log = Logger.get(CteScratchSweeper.class);

    // trailing query id in a scratch table name "cte_<name>_<candidateIndex>_<YYYYMMDD_HHmmss_counter_coord>"
    private static final Pattern QUERY_ID_SUFFIX = Pattern.compile("(\\d{8}_\\d{6}_\\d{5}_[0-9a-z]+)$");
    // query-id timestamps are formatted in UTC by QueryIdGenerator
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    private static final String SWEEPER_USER = "_cte_scratch_sweeper";

    private final List<String> schemas;
    private final long intervalMillis;
    private final Duration minAge;
    private final Metadata metadata;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;
    private final QueryIdGenerator queryIdGenerator;
    private final QueryManager queryManager;
    private final CteMaterializationOrchestrator orchestrator;
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("cte-scratch-sweeper-%s"));

    private ScheduledFuture<?> future;

    @Inject
    public CteScratchSweeper(
            CteMaterializationConfig config,
            Metadata metadata,
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionPropertyManager sessionPropertyManager,
            QueryIdGenerator queryIdGenerator,
            QueryManager queryManager,
            CteMaterializationOrchestrator orchestrator)
    {
        requireNonNull(config, "config is null");
        this.schemas = config.getOrphanSweepSchemas();
        this.intervalMillis = config.getOrphanSweepInterval().toMillis();
        this.minAge = Duration.ofMillis(config.getOrphanSweepMinAge().toMillis());
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.orchestrator = requireNonNull(orchestrator, "orchestrator is null");
    }

    @PostConstruct
    public void start()
    {
        if (schemas.isEmpty()) {
            log.info("CTE orphan-scratch sweeper disabled (no cte-materialization.orphan-sweep.schemas configured)");
            return;
        }
        future = executor.scheduleWithFixedDelay(this::sweepSafely, intervalMillis, intervalMillis, MILLISECONDS);
        log.info("CTE orphan-scratch sweeper enabled: schemas=%s interval=%sms min-age=%sms", schemas, intervalMillis, minAge.toMillis());
    }

    @PreDestroy
    public void stop()
    {
        if (future != null) {
            future.cancel(true);
        }
        executor.shutdownNow();
    }

    private void sweepSafely()
    {
        try {
            sweep(Instant.now());
            orchestrator.stats().sweepCompleted();
        }
        catch (Throwable t) {
            orchestrator.stats().sweepFailed();
            log.error(t, "CTE orphan-scratch sweep failed");
        }
    }

    private void sweep(Instant now)
    {
        for (String location : schemas) {
            List<String> parts = Splitter.on('.').splitToList(location);
            if (parts.size() != 2) {
                log.warn("CTE orphan sweep: ignoring invalid location '%s' (expected catalog.schema)", location);
                continue;
            }
            String catalog = parts.get(0);
            String schema = parts.get(1);

            List<QualifiedObjectName> tables;
            try {
                tables = TransactionBuilder.transaction(transactionManager, metadata, accessControl)
                        .readOnly()
                        .execute(internalSession(), txnSession -> {
                            return metadata.listTables(txnSession, new QualifiedTablePrefix(catalog, schema));
                        });
            }
            catch (RuntimeException e) {
                log.warn(e, "CTE orphan sweep: failed to list tables in %s", location);
                continue;
            }

            for (QualifiedObjectName table : tables) {
                if (!isSweepableOrphan(table.objectName(), now, minAge, this::queryState)) {
                    continue;
                }
                String fullName = table.catalogName() + "." + table.schemaName() + "." + table.objectName();
                try {
                    orchestrator.cleanup(internalSession(), fullName);
                    orchestrator.stats().orphanDropped();
                    log.info("CTE orphan sweep: dropped leaked scratch table %s", fullName);
                }
                catch (RuntimeException e) {
                    log.warn(e, "CTE orphan sweep: failed to drop %s", fullName);
                }
            }
        }
    }

    private Session internalSession()
    {
        return Session.builder(sessionPropertyManager)
                .setQueryId(queryIdGenerator.createNextQueryId())
                .setIdentity(Identity.ofUser(SWEEPER_USER))
                .setOriginalIdentity(Identity.ofUser(SWEEPER_USER))
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setLocale(ENGLISH)
                .build();
    }

    /**
     * A table is a sweepable orphan when its name is a scratch table ({@code cte_*}) carrying a parseable
     * query id, that query is NOT currently in a non-terminal (still-executing) state, and its query-id
     * timestamp is older than {@code minAge}. A still-running query's scratch is never dropped regardless
     * of age; a terminal (FINISHED/FAILED) query whose cleanup leaked, or a query unknown to this
     * coordinator (e.g. lost to a crash), becomes eligible once older than {@code minAge}. Unparseable
     * names and not-yet-aged tables are left alone. {@code queryState} returns empty when the query id is
     * unknown to this coordinator.
     */
    static boolean isSweepableOrphan(String tableName, Instant now, Duration minAge, Function<QueryId, Optional<QueryState>> queryState)
    {
        if (!tableName.startsWith("cte_")) {
            return false;
        }
        Optional<String> queryIdString = extractQueryId(tableName);
        if (queryIdString.isEmpty()) {
            return false;
        }
        QueryId queryId;
        try {
            queryId = QueryId.valueOf(queryIdString.get());
        }
        catch (IllegalArgumentException e) {
            return false;
        }
        Optional<QueryState> state = queryState.apply(queryId);
        if (state.isPresent() && !state.get().isDone()) {
            // the originating query is still executing on this coordinator — never drop its scratch,
            // no matter how long it runs
            return false;
        }
        Optional<Instant> created = queryIdTimestamp(queryIdString.get());
        // keep tables whose timestamp is missing/future or not yet older than minAge
        return created.isPresent() && Duration.between(created.get(), now).compareTo(minAge) >= 0;
    }

    private Optional<QueryState> queryState(QueryId queryId)
    {
        try {
            return Optional.of(queryManager.getQueryState(queryId));
        }
        catch (NoSuchElementException e) {
            return Optional.empty();
        }
    }

    static Optional<String> extractQueryId(String tableName)
    {
        Matcher matcher = QUERY_ID_SUFFIX.matcher(tableName);
        return matcher.find() ? Optional.of(matcher.group(1)) : Optional.empty();
    }

    static Optional<Instant> queryIdTimestamp(String queryId)
    {
        try {
            // first 15 chars are YYYYMMDD_HHmmss (UTC)
            LocalDateTime dateTime = LocalDateTime.parse(queryId.substring(0, 15), TIMESTAMP_FORMAT);
            return Optional.of(dateTime.toInstant(ZoneOffset.UTC));
        }
        catch (RuntimeException e) {
            return Optional.empty();
        }
    }
}
