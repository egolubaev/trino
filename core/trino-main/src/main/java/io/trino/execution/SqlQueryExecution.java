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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.CostCalculator;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsCalculator;
import io.trino.cte.CteMaterializationOrchestrator;
import io.trino.cte.CteMaterializationStrategy;
import io.trino.cte.CteMaterializer;
import io.trino.cte.CteMaterializer.CteCandidate;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.exchange.ExchangeMetricsCollector;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.PipelinedQueryScheduler;
import io.trino.execution.scheduler.QueryScheduler;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.execution.scheduler.TaskExecutionStats;
import io.trino.execution.scheduler.faulttolerant.EventDrivenFaultTolerantQueryScheduler;
import io.trino.execution.scheduler.faulttolerant.EventDrivenTaskSourceFactory;
import io.trino.execution.scheduler.faulttolerant.NodeAllocatorService;
import io.trino.execution.scheduler.faulttolerant.OutputStatsEstimatorFactory;
import io.trino.execution.scheduler.faulttolerant.PartitionMemoryEstimatorFactory;
import io.trino.execution.scheduler.faulttolerant.StageExecutionStats;
import io.trino.execution.scheduler.faulttolerant.TaskDescriptorStorage;
import io.trino.execution.scheduler.policy.ExecutionPolicy;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.node.InternalNodeManager;
import io.trino.operator.ForScheduler;
import io.trino.operator.RetryPolicy;
import io.trino.server.BasicQueryInfo;
import io.trino.server.DynamicFilterService;
import io.trino.server.ResultQueryInfo;
import io.trino.server.protocol.Slug;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.planner.AdaptivePlanner;
import io.trino.sql.planner.InputExtractor;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.PlanOptimizersFactory;
import io.trino.sql.planner.SplitSourceFactory;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.optimizations.AdaptivePlanOptimizer;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isEnableDynamicFiltering;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.PLANNING;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static io.trino.spi.StandardErrorCode.STACK_OVERFLOW;
import static io.trino.sql.planner.sanity.PlanSanityChecker.DISTRIBUTED_PLAN_SANITY_CHECKER;
import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.lang.Thread.currentThread;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class SqlQueryExecution
        implements QueryExecution
{
    private static final Logger log = Logger.get(SqlQueryExecution.class);

    private final QueryStateMachine stateMachine;
    private final Slug slug;
    private final Tracer tracer;
    private final PlannerContext plannerContext;
    private final SplitSourceFactory splitSourceFactory;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeScheduler nodeScheduler;
    private final NodeAllocatorService nodeAllocatorService;
    private final PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory;
    private final OutputStatsEstimatorFactory outputStatsEstimatorFactory;
    private final TaskExecutionStats taskExecutionStats;
    private final StageExecutionStats stageExecutionStats;
    private final List<PlanOptimizer> planOptimizers;
    private final List<AdaptivePlanOptimizer> adaptivePlanOptimizers;
    private final PlanFragmenter planFragmenter;
    private final RemoteTaskFactory remoteTaskFactory;
    private final int scheduleSplitBatchSize;
    private final ExecutorService queryExecutor;
    private final ScheduledExecutorService schedulerExecutor;
    private final InternalNodeManager nodeManager;

    private final AtomicReference<QueryScheduler> queryScheduler = new AtomicReference<>();
    private final AtomicReference<Plan> queryPlan = new AtomicReference<>();
    private final NodeTaskMap nodeTaskMap;
    private final ExecutionPolicy executionPolicy;
    private final SplitSchedulerStats schedulerStats;
    private final Analysis analysis;
    private final PreparedQuery preparedQuery;
    private final AnalyzerFactory analyzerFactory;
    private final CteMaterializationOrchestrator cteMaterializationOrchestrator;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final DynamicFilterService dynamicFilterService;
    private final TableExecuteContextManager tableExecuteContextManager;
    private final SqlTaskManager coordinatorTaskManager;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final ExchangeMetricsCollector exchangeMetricsCollector;
    private final EventDrivenTaskSourceFactory eventDrivenTaskSourceFactory;
    private final TaskDescriptorStorage taskDescriptorStorage;
    private final PlanOptimizersStatsCollector planOptimizersStatsCollector;

    private SqlQueryExecution(
            PreparedQuery preparedQuery,
            QueryStateMachine stateMachine,
            Slug slug,
            Tracer tracer,
            PlannerContext plannerContext,
            AnalyzerFactory analyzerFactory,
            CteMaterializationOrchestrator cteMaterializationOrchestrator,
            SplitSourceFactory splitSourceFactory,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            NodeAllocatorService nodeAllocatorService,
            PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory,
            OutputStatsEstimatorFactory outputStatsEstimatorFactory,
            TaskExecutionStats taskExecutionStats,
            StageExecutionStats stageExecutionStats,
            List<PlanOptimizer> planOptimizers,
            List<AdaptivePlanOptimizer> adaptivePlanOptimizers,
            PlanFragmenter planFragmenter,
            RemoteTaskFactory remoteTaskFactory,
            int scheduleSplitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            InternalNodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            DynamicFilterService dynamicFilterService,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            TableExecuteContextManager tableExecuteContextManager,
            SqlTaskManager coordinatorTaskManager,
            ExchangeManagerRegistry exchangeManagerRegistry,
            ExchangeMetricsCollector exchangeMetricsCollector,
            EventDrivenTaskSourceFactory eventDrivenTaskSourceFactory,
            TaskDescriptorStorage taskDescriptorStorage)
    {
        try (SetThreadName _ = new SetThreadName("Query-" + stateMachine.getQueryId())) {
            this.slug = requireNonNull(slug, "slug is null");
            this.tracer = requireNonNull(tracer, "tracer is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.nodeAllocatorService = requireNonNull(nodeAllocatorService, "nodeAllocatorService is null");
            this.partitionMemoryEstimatorFactory = requireNonNull(partitionMemoryEstimatorFactory, "partitionMemoryEstimatorFactory is null");
            this.outputStatsEstimatorFactory = requireNonNull(outputStatsEstimatorFactory, "outputDataSizeEstimatorFactory is null");
            this.taskExecutionStats = requireNonNull(taskExecutionStats, "taskExecutionStats is null");
            this.stageExecutionStats = requireNonNull(stageExecutionStats, "stageExecutionStats is null");
            this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
            this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");

            checkArgument(scheduleSplitBatchSize > 0, "scheduleSplitBatchSize must be greater than 0");
            this.scheduleSplitBatchSize = scheduleSplitBatchSize;

            this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");

            this.preparedQuery = requireNonNull(preparedQuery, "preparedQuery is null");
            this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
            this.cteMaterializationOrchestrator = requireNonNull(cteMaterializationOrchestrator, "cteMaterializationOrchestrator is null");

            // analyze query
            this.analysis = analyze(preparedQuery, stateMachine, warningCollector, planOptimizersStatsCollector, analyzerFactory);

            // for adaptive planner
            this.adaptivePlanOptimizers = ImmutableList.copyOf(requireNonNull(adaptivePlanOptimizers, "adaptivePlanOptimizers is null"));

            stateMachine.addStateChangeListener(state -> {
                if (!state.isDone()) {
                    return;
                }
                unregisterDynamicFilteringQuery(
                        dynamicFilterService.getDynamicFilteringStats(stateMachine.getQueryId()));

                tableExecuteContextManager.unregisterTableExecuteContextForQuery(stateMachine.getQueryId());
            });

            this.remoteTaskFactory = new MemoryTrackingRemoteTaskFactory(requireNonNull(remoteTaskFactory, "remoteTaskFactory is null"), stateMachine);
            this.coordinatorTaskManager = requireNonNull(coordinatorTaskManager, "coordinatorTaskManager is null");
            this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
            this.exchangeMetricsCollector = requireNonNull(exchangeMetricsCollector, "exchangeMetricsCollector is null");
            this.eventDrivenTaskSourceFactory = requireNonNull(eventDrivenTaskSourceFactory, "taskSourceFactory is null");
            this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
            this.planOptimizersStatsCollector = requireNonNull(planOptimizersStatsCollector, "planOptimizersStatsCollector is null");
        }
    }

    private synchronized void registerDynamicFilteringQuery(PlanRoot plan)
    {
        if (!isEnableDynamicFiltering(stateMachine.getSession())) {
            return;
        }

        if (isDone()) {
            // query has finished or was cancelled asynchronously
            return;
        }

        dynamicFilterService.registerQuery(getSession(), getQueryPlan().orElseThrow().getRoot(), plan.getRoot());
        stateMachine.setDynamicFiltersStatsSupplier(
                () -> dynamicFilterService.getDynamicFilteringStats(stateMachine.getQueryId()));
    }

    private synchronized void unregisterDynamicFilteringQuery(DynamicFiltersStats finalDynamicFiltersStats)
    {
        checkState(isDone(), "Expected query to be in done state");
        stateMachine.setDynamicFiltersStatsSupplier(() -> finalDynamicFiltersStats);
        dynamicFilterService.removeQuery(stateMachine.getQueryId());
    }

    private static Analysis analyze(
            PreparedQuery preparedQuery,
            QueryStateMachine stateMachine,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            AnalyzerFactory analyzerFactory)
    {
        stateMachine.beginAnalysis();

        requireNonNull(preparedQuery, "preparedQuery is null");
        Analyzer analyzer = analyzerFactory.createAnalyzer(
                stateMachine.getSession(),
                preparedQuery.getParameters(),
                bindParameters(preparedQuery.getStatement(), preparedQuery.getParameters()),
                warningCollector,
                planOptimizersStatsCollector);
        Analysis analysis;
        try {
            analysis = analyzer.analyze(preparedQuery.getStatement());
        }
        catch (StackOverflowError e) {
            throw new TrinoException(STACK_OVERFLOW, "statement is too large (stack overflow during analysis)", e);
        }

        stateMachine.setUpdateType(analysis.getUpdateType());
        stateMachine.setReferencedTables(analysis.getReferencedTables());
        stateMachine.setRoutines(analysis.getRoutines());
        stateMachine.setSelectColumnsLineageInfo(analysis.getSelectColumnsLineageInfo());

        stateMachine.endAnalysis();

        return analysis;
    }

    @Override
    public Slug getSlug()
    {
        return slug;
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        // acquire reference to scheduler before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears scheduler when
        // the query finishes.
        QueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getUserMemoryReservation();
        }
        if (scheduler == null) {
            return DataSize.ofBytes(0);
        }
        return succinctBytes(scheduler.getUserMemoryReservation());
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        // acquire reference to scheduler before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears scheduler when
        // the query finishes.
        QueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalMemoryReservation();
        }
        if (scheduler == null) {
            return DataSize.ofBytes(0);
        }
        return succinctBytes(scheduler.getTotalMemoryReservation());
    }

    @Override
    public Instant getCreateTime()
    {
        return stateMachine.getCreateTime();
    }

    @Override
    public Optional<Instant> getExecutionStartTime()
    {
        return stateMachine.getExecutionStartTime();
    }

    @Override
    public Optional<Duration> getPlanningTime()
    {
        return stateMachine.getPlanningTime();
    }

    @Override
    public Instant getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public Optional<Instant> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        QueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalCpuTime();
        }
        if (scheduler == null) {
            return succinctDuration(0, MILLISECONDS);
        }
        return scheduler.getTotalCpuTime();
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return stateMachine.getFinalQueryInfo()
                .map(BasicQueryInfo::new)
                .orElseGet(() -> stateMachine.getBasicQueryInfo(Optional.ofNullable(queryScheduler.get()).map(QueryScheduler::getBasicStageStats)));
    }

    @Override
    public void start()
    {
        try (SetThreadName _ = new SetThreadName("Query-" + stateMachine.getQueryId())) {
            try {
                if (!stateMachine.transitionToPlanning()) {
                    // query already started or finished
                    return;
                }

                AtomicReference<Thread> planningThread = new AtomicReference<>(currentThread());
                stateMachine.getStateChange(PLANNING).addListener(() -> {
                    if (stateMachine.getQueryState() == FAILED) {
                        synchronized (planningThread) {
                            Thread thread = planningThread.get();
                            if (thread != null) {
                                thread.interrupt();
                            }
                        }
                    }
                }, directExecutor());

                try {
                    CachingTableStatsProvider tableStatsProvider = new CachingTableStatsProvider(plannerContext.getMetadata(), getSession(), stateMachine::isDone);
                    PlanRoot plan = planQuery(tableStatsProvider);
                    // DynamicFilterService needs plan for query to be registered.
                    // Query should be registered before dynamic filter suppliers are requested in distribution planning.
                    registerDynamicFilteringQuery(plan);
                    planDistribution(plan, tableStatsProvider);
                }
                finally {
                    synchronized (planningThread) {
                        planningThread.set(null);
                        // Clear the interrupted flag in case there was a race condition where
                        // the planning thread was interrupted right after planning completes above
                        Thread.interrupted();
                    }
                }

                tableExecuteContextManager.registerTableExecuteContextForQuery(getQueryId());

                if (!stateMachine.transitionToStarting()) {
                    // query already started or finished
                    return;
                }

                // if query is not finished, start the scheduler, otherwise cancel it
                QueryScheduler scheduler = queryScheduler.get();

                if (!stateMachine.isDone()) {
                    scheduler.start();
                }
            }
            catch (Throwable e) {
                fail(e);
                throwIfInstanceOf(e, Error.class);
            }
        }
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        try (SetThreadName _ = new SetThreadName("Query-" + stateMachine.getQueryId())) {
            stateMachine.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        stateMachine.addQueryInfoStateChangeListener(stateChangeListener);
    }

    private PlanRoot planQuery(CachingTableStatsProvider tableStatsProvider)
    {
        Span span = tracer.spanBuilder("planner")
                .setParent(Context.current().with(getSession().getQuerySpan()))
                .startSpan();
        try (var _ = scopedSpan(span)) {
            return doPlanQuery(tableStatsProvider);
        }
        catch (StackOverflowError e) {
            throw new TrinoException(STACK_OVERFLOW, "statement is too large (stack overflow during analysis)", e);
        }
    }

    private PlanRoot doPlanQuery(CachingTableStatsProvider tableStatsProvider)
    {
        // optionally materialize multiply-referenced CTEs into scratch tables and re-analyze
        Analysis planAnalysis = maybeMaterializeCtes(tableStatsProvider);

        // plan query
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        LogicalPlanner logicalPlanner = new LogicalPlanner(
                stateMachine.getSession(),
                planOptimizers,
                idAllocator,
                plannerContext,
                statsCalculator,
                costCalculator,
                stateMachine.getWarningCollector(),
                planOptimizersStatsCollector,
                tableStatsProvider);
        Plan plan = logicalPlanner.plan(planAnalysis);
        queryPlan.set(plan);

        // fragment the plan
        SubPlan fragmentedPlan;
        try (var _ = scopedSpan(tracer, "fragment-plan")) {
            fragmentedPlan = planFragmenter.createSubPlans(stateMachine.getSession(), plan, false, stateMachine.getWarningCollector());
        }

        // extract inputs
        try (var _ = scopedSpan(tracer, "extract-inputs")) {
            stateMachine.setInputs(new InputExtractor(plannerContext.getMetadata(), stateMachine.getSession()).extractInputs(fragmentedPlan));
        }

        stateMachine.setOutput(planAnalysis.getTarget());

        boolean explainAnalyze = planAnalysis.getStatement() instanceof ExplainAnalyze;
        return new PlanRoot(fragmentedPlan, !explainAnalyze);
    }

    /**
     * If CTE materialization is enabled and the statement has eligible multiply-referenced CTEs,
     * materialize each into a per-query scratch table (committed in its own transaction), rewrite the
     * statement to read the scratch tables, and return a fresh analysis of the rewritten statement.
     * On any failure the feature degrades to normal inlining (returns the original analysis); scratch
     * tables created before the failure are dropped by the terminal-state cleanup listener.
     */
    private Analysis maybeMaterializeCtes(CachingTableStatsProvider tableStatsProvider)
    {
        Session session = stateMachine.getSession();
        CteMaterializationStrategy strategy = SystemSessionProperties.getCteMaterializationStrategy(session);
        if (strategy == CteMaterializationStrategy.NONE) {
            return analysis;
        }
        Statement statement = preparedQuery.getStatement();
        SqlParser parser = new SqlParser();
        List<CteCandidate> candidates = selectCandidates(CteMaterializer.findCandidates(statement), strategy, session, statement, parser, tableStatsProvider);
        if (candidates.isEmpty()) {
            return analysis;
        }
        Map<String, String> nameToScratch = new LinkedHashMap<>();
        long estimatedRowsSaved = 0;
        try {
            // assign each candidate a scratch table (by source-catalog placement); candidates with no usable
            // location are inlined. The candidate's list index keeps colliding sanitized names distinct.
            Map<String, String> scratchTableByName = new LinkedHashMap<>();
            for (int candidateIndex = 0; candidateIndex < candidates.size(); candidateIndex++) {
                CteCandidate candidate = candidates.get(candidateIndex);
                Optional<String> scratchSchema = scratchSchemaForCte(session, statement, candidate.name());
                if (scratchSchema.isEmpty()) {
                    // no scratch location for this CTE (no qualified source and no default schema): inline it
                    // (a dependent CTE that is materialized will inline this one into its scratch CTAS)
                    log.debug("CTE materialization: no scratch location for %s in query %s; inlining it",
                            candidate.name(), stateMachine.getQueryId());
                    cteMaterializationOrchestrator.stats().cteInlinedNoLocation();
                    continue;
                }
                scratchTableByName.put(candidate.name(), scratchTableName(scratchSchema.get(), candidate.name(), session.getQueryId().getId(), candidateIndex));
                // estimated repeated-scan rows avoided, summed over CTEs whose source size is known
                OptionalDouble sourceRows = estimateSourceRows(session, statement, candidate.name());
                if (sourceRows.isPresent()) {
                    estimatedRowsSaved += (long) ((candidate.referenceCount() - 1) * sourceRows.getAsDouble());
                }
            }
            if (scratchTableByName.isEmpty()) {
                // no candidate had a usable scratch location: inline the whole statement
                return analysis;
            }
            // materialize by dependency level: CTEs in one level are independent and run concurrently
            // (bounded by max_concurrent_materializations); a dependency's level commits before its dependents'.
            int maxConcurrent = SystemSessionProperties.getCteMaterializationMaxConcurrentMaterializations(session);
            List<String> toMaterialize = new ArrayList<>(scratchTableByName.keySet());
            for (List<String> level : CteMaterializer.dependencyLevels(statement, toMaterialize)) {
                List<String> levelTables = new ArrayList<>();
                List<String> levelSources = new ArrayList<>();
                for (String name : level) {
                    String scratchTable = scratchTableByName.get(name);
                    // prior levels are already in nameToScratch, so a dependent's source reads their scratch tables
                    String scratchSource = CteMaterializer.buildScratchSource(statement, name, nameToScratch, parser);
                    // register cleanup before running so a later failure still drops this table
                    registerScratchCleanup(session, scratchTable);
                    levelTables.add(scratchTable);
                    levelSources.add(scratchSource);
                }
                cteMaterializationOrchestrator.materializeLevel(session, levelTables, levelSources, maxConcurrent);
                for (int i = 0; i < level.size(); i++) {
                    nameToScratch.put(level.get(i), levelTables.get(i));
                }
            }
            Statement rewritten = CteMaterializer.rewrite(statement, nameToScratch, parser);
            Analyzer analyzer = analyzerFactory.createAnalyzer(
                    session,
                    preparedQuery.getParameters(),
                    bindParameters(rewritten, preparedQuery.getParameters()),
                    stateMachine.getWarningCollector(),
                    planOptimizersStatsCollector);
            Analysis rewrittenAnalysis = analyzer.analyze(rewritten);
            cteMaterializationOrchestrator.stats().queryMaterialized(nameToScratch.size(), estimatedRowsSaved);
            log.info("CTE materialization: query %s materialized %s CTE(s) into scratch tables %s",
                    stateMachine.getQueryId(), nameToScratch.size(), nameToScratch.values());
            return rewrittenAnalysis;
        }
        catch (RuntimeException e) {
            // the feature must never break a query: fall back to inlining the original statement
            cteMaterializationOrchestrator.stats().materializationFallback();
            log.warn(e, "CTE materialization failed for query %s; falling back to inlining", stateMachine.getQueryId());
            return analysis;
        }
    }

    /**
     * Apply the configured strategy to the set of eligible (multiply-referenced, safe) candidates.
     * {@code ALL} keeps every eligible CTE. {@code HEURISTIC} keeps a CTE only when it is referenced at
     * least {@code cte_materialization_min_references} times, its estimated repeated-scan savings —
     * {@code (referenceCount - 1) * sourceRows} — reach {@code cte_materialization_min_scan_savings}, AND its
     * estimated output does not exceed {@code cte_materialization_max_output_rows}. Input savings and output
     * size are each treated as unknown (and the CTE is kept) when statistics are unavailable, so the gate
     * never withholds materialization on missing stats; it only ever declines when it can show the repeated
     * scan is small or the scratch table would be large.
     */
    private List<CteCandidate> selectCandidates(List<CteCandidate> eligible, CteMaterializationStrategy strategy, Session session, Statement statement, SqlParser parser, CachingTableStatsProvider tableStatsProvider)
    {
        List<CteCandidate> gated;
        if (strategy == CteMaterializationStrategy.ALL) {
            gated = eligible;
        }
        else {
            int minReferences = SystemSessionProperties.getCteMaterializationMinReferences(session);
            long minScanSavings = SystemSessionProperties.getCteMaterializationMinScanSavings(session);
            long maxOutputRows = SystemSessionProperties.getCteMaterializationMaxOutputRows(session);
            ImmutableList.Builder<CteCandidate> selected = ImmutableList.builder();
            for (CteCandidate candidate : eligible) {
                if (candidate.referenceCount() < minReferences) {
                    continue;
                }
                OptionalDouble sourceRows = estimateSourceRows(session, statement, candidate.name());
                if (sourceRows.isPresent()) {
                    double savings = (candidate.referenceCount() - 1) * sourceRows.getAsDouble();
                    if (savings < minScanSavings) {
                        log.debug("CTE materialization: skipping %s (estimated saved scan rows %.0f < threshold %s)",
                                candidate.name(), savings, minScanSavings);
                        continue;
                    }
                }
                // output-size gate: a CTE whose result is large is expensive to write and re-read as a scratch
                // table, reads back with little parallelism (few files), and loses predicate/dynamic-filter
                // pushdown into its consumers — re-scanning is usually cheaper. Skip when the estimate exceeds
                // the limit; an unknown estimate never blocks (fail-open).
                if (maxOutputRows > 0) {
                    OptionalDouble outputRows = estimateOutputRows(session, statement, candidate.name(), parser, tableStatsProvider);
                    if (outputRows.isPresent() && outputRows.getAsDouble() > maxOutputRows) {
                        log.debug("CTE materialization: skipping %s (estimated output rows %.0f > threshold %s)",
                                candidate.name(), outputRows.getAsDouble(), maxOutputRows);
                        continue;
                    }
                }
                selected.add(candidate);
            }
            gated = selected.build();
        }
        return capCandidates(gated, session);
    }

    /**
     * Estimate the number of rows a CTE produces, for the HEURISTIC output-size gate. The CTE body (with its
     * dependency closure inlined, exactly as it would be materialized) is analyzed and planned to the initial
     * {@code CREATED} stage — no optimizer passes — and the cost-based row estimate of the plan root is read.
     * Empty when the estimate is unknown or anything fails, so the caller never blocks on it.
     */
    private OptionalDouble estimateOutputRows(Session session, Statement statement, String cteName, SqlParser parser, CachingTableStatsProvider tableStatsProvider)
    {
        try {
            Statement cteStatement = parser.createStatement(CteMaterializer.buildScratchSource(statement, cteName, Map.of(), parser));
            Analyzer analyzer = analyzerFactory.createAnalyzer(
                    session,
                    preparedQuery.getParameters(),
                    bindParameters(cteStatement, preparedQuery.getParameters()),
                    WarningCollector.NOOP,
                    planOptimizersStatsCollector);
            Analysis cteAnalysis = analyzer.analyze(cteStatement);
            LogicalPlanner planner = new LogicalPlanner(
                    session,
                    planOptimizers,
                    new PlanNodeIdAllocator(),
                    plannerContext,
                    statsCalculator,
                    costCalculator,
                    WarningCollector.NOOP,
                    planOptimizersStatsCollector,
                    tableStatsProvider);
            // collectPlanStatistics=true: the CREATED stage runs no optimizer to populate the stats cache,
            // so fetch table statistics fresh, otherwise the root estimate would be unknown
            Plan plan = planner.plan(cteAnalysis, LogicalPlanner.Stage.CREATED, true);
            PlanNodeStatsEstimate rootStats = plan.getStatsAndCosts().getStats().get(plan.getRoot().getId());
            if (rootStats == null) {
                return OptionalDouble.empty();
            }
            double outputRowCount = rootStats.getOutputRowCount();
            return Double.isNaN(outputRowCount) ? OptionalDouble.empty() : OptionalDouble.of(outputRowCount);
        }
        catch (RuntimeException e) {
            // any analysis/planning failure -> unknown, do not prune
            log.debug(e, "CTE materialization: could not estimate output rows for %s; treating as unknown", cteName);
            return OptionalDouble.empty();
        }
    }

    /**
     * Cap the number of materialized CTEs at {@code cte_materialization_max_materialized_ctes}. When more
     * candidates qualify, keep those with the most references (the largest repeated-scan wins), breaking ties
     * by declaration order, and inline the rest. The kept candidates are returned in declaration order so the
     * materialization loop still sees dependencies before dependents. Dropping a dependency is safe: a kept
     * dependent simply inlines it into its own scratch CTAS.
     */
    private List<CteCandidate> capCandidates(List<CteCandidate> gated, Session session)
    {
        int cap = SystemSessionProperties.getCteMaterializationMaxMaterializedCtes(session);
        if (gated.size() <= cap) {
            return gated;
        }
        List<CteCandidate> byReferences = new ArrayList<>(gated);
        byReferences.sort(Comparator.comparingInt(CteCandidate::referenceCount).reversed());
        Set<String> keep = new HashSet<>();
        for (CteCandidate candidate : byReferences.subList(0, cap)) {
            keep.add(candidate.name());
        }
        log.debug("CTE materialization: capping materialized CTEs at %s of %s eligible for query %s",
                cap, gated.size(), stateMachine.getQueryId());
        ImmutableList.Builder<CteCandidate> capped = ImmutableList.builder();
        for (CteCandidate candidate : gated) {
            if (keep.contains(candidate.name())) {
                capped.add(candidate);
            }
        }
        return capped.build();
    }

    /**
     * Sum of base-table row counts scanned by a CTE body, for the HEURISTIC cost gate. Empty when the CTE
     * depends on another CTE, when a table cannot be resolved, or when any table's row-count statistic is
     * unknown — i.e. whenever we cannot confidently size the scan (the caller then declines to prune).
     */
    private OptionalDouble estimateSourceRows(Session session, Statement statement, String cteName)
    {
        Optional<List<QualifiedName>> tables = CteMaterializer.sourceTablesForCostEstimate(statement, cteName);
        if (tables.isEmpty()) {
            return OptionalDouble.empty();
        }
        Metadata metadata = plannerContext.getMetadata();
        double total = 0;
        try {
            for (QualifiedName table : tables.get()) {
                QualifiedObjectName name = createQualifiedObjectName(session, statement, table);
                Optional<TableHandle> handle = metadata.getTableHandle(session, name);
                if (handle.isEmpty()) {
                    return OptionalDouble.empty();
                }
                Estimate rowCount = metadata.getTableStatistics(session, handle.get()).getRowCount();
                if (rowCount.isUnknown()) {
                    return OptionalDouble.empty();
                }
                total += rowCount.getValue();
            }
        }
        catch (RuntimeException e) {
            // any resolution/stats failure -> unknown, do not prune
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(total);
    }

    /**
     * Catalog.schema in which to create the scratch table for one CTE. The scratch table is placed in the
     * same catalog as the data the CTE reads — its first fully-qualified ({@code catalog.schema.table})
     * source table — so a CTE reading a non-Iceberg catalog is not materialized into Iceberg. When the CTE
     * references no qualified table (everything resolves through the session default), the session's default
     * catalog+schema is used; failing that, any fully-qualified source table in the statement. The resulting
     * source catalog is then redirected to its configured {@code cte-materialization.scratch-schemas} target
     * (e.g. a dedicated {@code temp_cte} schema) when one is set. Empty when no location can be determined.
     */
    private Optional<String> scratchSchemaForCte(Session session, Statement statement, String cteName)
    {
        Optional<String> sourceCatalogSchema = CteMaterializer.firstQualifiedTableForCte(statement, cteName)
                .map(name -> name.getParts().get(0) + "." + name.getParts().get(1));
        if (sourceCatalogSchema.isEmpty() && session.getCatalog().isPresent() && session.getSchema().isPresent()) {
            sourceCatalogSchema = Optional.of(session.getCatalog().get() + "." + session.getSchema().get());
        }
        if (sourceCatalogSchema.isEmpty()) {
            sourceCatalogSchema = CteMaterializer.firstQualifiedTable(statement)
                    .map(name -> name.getParts().get(0) + "." + name.getParts().get(1));
        }
        return sourceCatalogSchema.map(catalogSchema -> {
            String sourceCatalog = catalogSchema.substring(0, catalogSchema.indexOf('.'));
            return cteMaterializationOrchestrator.scratchSchemaOverride(sourceCatalog).orElse(catalogSchema);
        });
    }

    /**
     * Scratch table name {@code cte_<sanitized-cte-name>_<candidate-index>_<query-id>}. The candidate index
     * disambiguates CTE names that sanitize to the same string (e.g. {@code "a-b"} and {@code a_b} both
     * become {@code a_b}); without it the second CTAS would collide ("table already exists") and abort the
     * whole materialization. The query id stays last so the orphan sweeper can still extract it from the
     * trailing portion of the name.
     */
    private static String scratchTableName(String scratchSchema, String cteName, String queryId, int candidateIndex)
    {
        String sanitized = cteName.toLowerCase(ENGLISH).replaceAll("[^a-z0-9_]", "_");
        return scratchSchema + ".cte_" + sanitized + "_" + candidateIndex + "_" + queryId;
    }

    private void registerScratchCleanup(Session session, String scratchTable)
    {
        stateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                cteMaterializationOrchestrator.cleanupAsync(session, scratchTable);
            }
        });
    }

    private void planDistribution(PlanRoot plan, CachingTableStatsProvider tableStatsProvider)
    {
        // if query was canceled, skip creating scheduler
        if (stateMachine.isDone()) {
            return;
        }

        // record output field
        PlanFragment rootFragment = plan.getRoot().getFragment();
        stateMachine.setColumns(
                ((OutputNode) rootFragment.getRoot()).getColumnNames(),
                rootFragment.getTypes());

        RetryPolicy retryPolicy = getRetryPolicy(getSession());
        QueryScheduler scheduler = switch (retryPolicy) {
            case QUERY, NONE -> new PipelinedQueryScheduler(
                    stateMachine,
                    plan.getRoot(),
                    nodePartitioningManager,
                    nodeScheduler,
                    remoteTaskFactory,
                    plan.isSummarizeTaskInfos(),
                    scheduleSplitBatchSize,
                    queryExecutor,
                    schedulerExecutor,
                    nodeManager,
                    nodeTaskMap,
                    executionPolicy,
                    tracer,
                    schedulerStats,
                    dynamicFilterService,
                    tableExecuteContextManager,
                    plannerContext.getMetadata(),
                    splitSourceFactory,
                    coordinatorTaskManager);
            case TASK -> new EventDrivenFaultTolerantQueryScheduler(
                    stateMachine,
                    plannerContext.getMetadata(),
                    remoteTaskFactory,
                    taskDescriptorStorage,
                    eventDrivenTaskSourceFactory,
                    plan.isSummarizeTaskInfos(),
                    nodeTaskMap,
                    queryExecutor,
                    schedulerExecutor,
                    tracer,
                    schedulerStats,
                    partitionMemoryEstimatorFactory,
                    outputStatsEstimatorFactory,
                    nodePartitioningManager,
                    exchangeManagerRegistry.getExchangeManager(),
                    exchangeMetricsCollector,
                    nodeAllocatorService,
                    nodeManager,
                    dynamicFilterService,
                    taskExecutionStats,
                    new AdaptivePlanner(
                            stateMachine.getSession(),
                            plannerContext,
                            adaptivePlanOptimizers,
                            planFragmenter,
                            DISTRIBUTED_PLAN_SANITY_CHECKER,
                            stateMachine.getWarningCollector(),
                            planOptimizersStatsCollector,
                            tableStatsProvider),
                    stageExecutionStats,
                    plan.getRoot());
        };

        queryScheduler.set(scheduler);
        stateMachine.addQueryInfoStateChangeListener(queryInfo -> {
            if (queryInfo.isFinalQueryInfo()) {
                queryScheduler.set(null);
            }
        });
    }

    @Override
    public void cancelQuery()
    {
        stateMachine.transitionToCanceled();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");

        try (SetThreadName _ = new SetThreadName("Query-" + stateMachine.getQueryId())) {
            QueryScheduler scheduler = queryScheduler.get();
            if (scheduler != null) {
                scheduler.cancelStage(stageId);
            }
        }
    }

    @Override
    public void failTask(TaskId taskId, Exception reason)
    {
        requireNonNull(taskId, "stageId is null");

        try (SetThreadName _ = new SetThreadName("Query-" + stateMachine.getQueryId())) {
            QueryScheduler scheduler = queryScheduler.get();
            if (scheduler != null) {
                scheduler.failTask(taskId, reason);
            }
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        stateMachine.transitionToFailed(cause);
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public void setOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        stateMachine.setOutputInfoListener(listener);
    }

    @Override
    public void outputTaskFailed(TaskId taskId, Throwable failure)
    {
        stateMachine.outputTaskFailed(taskId, failure);
    }

    @Override
    public void resultsConsumed()
    {
        stateMachine.resultsConsumed();
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return stateMachine.getStateChange(currentState);
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public void pruneInfo()
    {
        stateMachine.pruneQueryInfo();
    }

    @Override
    public boolean isInfoPruned()
    {
        return stateMachine.isQueryInfoPruned();
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        try (SetThreadName _ = new SetThreadName("Query-" + stateMachine.getQueryId())) {
            // acquire reference to scheduler before checking finalQueryInfo, because
            // state change listener sets finalQueryInfo and then clears scheduler when
            // the query finishes.
            QueryScheduler scheduler = queryScheduler.get();

            return stateMachine.getFinalQueryInfo().orElseGet(() -> buildQueryInfo(scheduler));
        }
    }

    @Override
    public ResultQueryInfo getResultQueryInfo()
    {
        Optional<QueryScheduler> scheduler = Optional.ofNullable(queryScheduler.get());
        return stateMachine.getFinalQueryInfo()
                .map(ResultQueryInfo::new)
                .orElseGet(() -> stateMachine.updateResultQueryInfo(
                        scheduler.map(QueryScheduler::getBasicStagesInfo),
                        () -> scheduler.map(QueryScheduler::getStagesInfo)));
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    @Override
    public Optional<Plan> getQueryPlan()
    {
        return Optional.ofNullable(queryPlan.get());
    }

    private QueryInfo buildQueryInfo(QueryScheduler scheduler)
    {
        Optional<StagesInfo> stagesInfo = Optional.empty();
        if (scheduler != null) {
            stagesInfo = Optional.ofNullable(scheduler.getStagesInfo());
        }
        return stateMachine.updateQueryInfo(stagesInfo);
    }

    @Override
    public boolean shouldWaitForMinWorkers()
    {
        return shouldWaitForMinWorkers(analysis.getStatement());
    }

    private boolean shouldWaitForMinWorkers(Statement statement)
    {
        if (statement instanceof Query) {
            // Allow set session statements and queries on internal system connectors to run without waiting
            Collection<TableHandle> tables = analysis.getTables();
            return !tables.stream()
                    .map(TableHandle::catalogHandle)
                    .allMatch(catalogName -> catalogName.getType().isInternal());
        }
        return true;
    }

    private static class PlanRoot
    {
        private final SubPlan root;
        private final boolean summarizeTaskInfos;

        public PlanRoot(SubPlan root, boolean summarizeTaskInfos)
        {
            this.root = requireNonNull(root, "root is null");
            this.summarizeTaskInfos = summarizeTaskInfos;
        }

        public SubPlan getRoot()
        {
            return root;
        }

        public boolean isSummarizeTaskInfos()
        {
            return summarizeTaskInfos;
        }
    }

    public static class SqlQueryExecutionFactory
            implements QueryExecutionFactory<QueryExecution>
    {
        private final Tracer tracer;
        private final SplitSchedulerStats schedulerStats;
        private final int scheduleSplitBatchSize;
        private final PlannerContext plannerContext;
        private final AnalyzerFactory analyzerFactory;
        private final CteMaterializationOrchestrator cteMaterializationOrchestrator;
        private final SplitSourceFactory splitSourceFactory;
        private final NodePartitioningManager nodePartitioningManager;
        private final NodeScheduler nodeScheduler;
        private final NodeAllocatorService nodeAllocatorService;
        private final PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory;
        private final OutputStatsEstimatorFactory outputStatsEstimatorFactory;
        private final TaskExecutionStats taskExecutionStats;
        private final StageExecutionStats stageExecutionStats;
        private final List<PlanOptimizer> planOptimizers;
        private final List<AdaptivePlanOptimizer> adaptivePlanOptimizers;
        private final PlanFragmenter planFragmenter;
        private final RemoteTaskFactory remoteTaskFactory;
        private final ExecutorService queryExecutor;
        private final ScheduledExecutorService schedulerExecutor;
        private final InternalNodeManager nodeManager;
        private final NodeTaskMap nodeTaskMap;
        private final Map<String, ExecutionPolicy> executionPolicies;
        private final StatsCalculator statsCalculator;
        private final CostCalculator costCalculator;
        private final DynamicFilterService dynamicFilterService;
        private final TableExecuteContextManager tableExecuteContextManager;
        private final SqlTaskManager coordinatorTaskManager;
        private final ExchangeManagerRegistry exchangeManagerRegistry;
        private final ExchangeMetricsCollector exchangeMetricsCollector;
        private final EventDrivenTaskSourceFactory eventDrivenTaskSourceFactory;
        private final TaskDescriptorStorage taskDescriptorStorage;

        @Inject
        SqlQueryExecutionFactory(
                Tracer tracer,
                QueryManagerConfig config,
                PlannerContext plannerContext,
                AnalyzerFactory analyzerFactory,
                CteMaterializationOrchestrator cteMaterializationOrchestrator,
                SplitSourceFactory splitSourceFactory,
                NodePartitioningManager nodePartitioningManager,
                NodeScheduler nodeScheduler,
                NodeAllocatorService nodeAllocatorService,
                PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory,
                OutputStatsEstimatorFactory outputStatsEstimatorFactory,
                TaskExecutionStats taskExecutionStats,
                StageExecutionStats stageExecutionStats,
                PlanOptimizersFactory planOptimizersFactory,
                PlanFragmenter planFragmenter,
                RemoteTaskFactory remoteTaskFactory,
                @ForQueryExecution ExecutorService queryExecutor,
                @ForScheduler ScheduledExecutorService schedulerExecutor,
                InternalNodeManager nodeManager,
                NodeTaskMap nodeTaskMap,
                Map<String, ExecutionPolicy> executionPolicies,
                SplitSchedulerStats schedulerStats,
                StatsCalculator statsCalculator,
                CostCalculator costCalculator,
                DynamicFilterService dynamicFilterService,
                TableExecuteContextManager tableExecuteContextManager,
                SqlTaskManager coordinatorTaskManager,
                ExchangeManagerRegistry exchangeManagerRegistry,
                ExchangeMetricsCollector exchangeMetricsCollector,
                EventDrivenTaskSourceFactory eventDrivenTaskSourceFactory,
                TaskDescriptorStorage taskDescriptorStorage)
        {
            this.tracer = requireNonNull(tracer, "tracer is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.scheduleSplitBatchSize = config.getScheduleSplitBatchSize();
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
            this.cteMaterializationOrchestrator = requireNonNull(cteMaterializationOrchestrator, "cteMaterializationOrchestrator is null");
            this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.nodeAllocatorService = requireNonNull(nodeAllocatorService, "nodeAllocatorService is null");
            this.partitionMemoryEstimatorFactory = requireNonNull(partitionMemoryEstimatorFactory, "partitionMemoryEstimatorFactory is null");
            this.outputStatsEstimatorFactory = requireNonNull(outputStatsEstimatorFactory, "outputDataSizeEstimatorFactory is null");
            this.taskExecutionStats = requireNonNull(taskExecutionStats, "taskExecutionStats is null");
            this.stageExecutionStats = requireNonNull(stageExecutionStats, "stageExecutionStats is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
            this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.executionPolicies = requireNonNull(executionPolicies, "executionPolicies is null");
            requireNonNull(planOptimizersFactory, "planOptimizersFactory is null");
            this.planOptimizers = planOptimizersFactory.getPlanOptimizers();
            this.adaptivePlanOptimizers = planOptimizersFactory.getAdaptivePlanOptimizers();
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
            this.coordinatorTaskManager = requireNonNull(coordinatorTaskManager, "coordinatorTaskManager is null");
            this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
            this.exchangeMetricsCollector = requireNonNull(exchangeMetricsCollector, "exchangeMetricsCollector is null");
            this.eventDrivenTaskSourceFactory = requireNonNull(eventDrivenTaskSourceFactory, "eventDrivenTaskSourceFactory is null");
            this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
        }

        @Override
        public QueryExecution createQueryExecution(
                PreparedQuery preparedQuery,
                QueryStateMachine stateMachine,
                Slug slug,
                WarningCollector warningCollector,
                PlanOptimizersStatsCollector planOptimizersStatsCollector)
        {
            String executionPolicyName = SystemSessionProperties.getExecutionPolicy(stateMachine.getSession());
            ExecutionPolicy executionPolicy = executionPolicies.get(executionPolicyName);
            checkArgument(executionPolicy != null, "No execution policy %s", executionPolicyName);

            return new SqlQueryExecution(
                    preparedQuery,
                    stateMachine,
                    slug,
                    tracer,
                    plannerContext,
                    analyzerFactory,
                    cteMaterializationOrchestrator,
                    splitSourceFactory,
                    nodePartitioningManager,
                    nodeScheduler,
                    nodeAllocatorService,
                    partitionMemoryEstimatorFactory,
                    outputStatsEstimatorFactory,
                    taskExecutionStats,
                    stageExecutionStats,
                    planOptimizers,
                    adaptivePlanOptimizers,
                    planFragmenter,
                    remoteTaskFactory,
                    scheduleSplitBatchSize,
                    queryExecutor,
                    schedulerExecutor,
                    nodeManager,
                    nodeTaskMap,
                    executionPolicy,
                    schedulerStats,
                    statsCalculator,
                    costCalculator,
                    dynamicFilterService,
                    warningCollector,
                    planOptimizersStatsCollector,
                    tableExecuteContextManager,
                    coordinatorTaskManager,
                    exchangeManagerRegistry,
                    exchangeMetricsCollector,
                    eventDrivenTaskSourceFactory,
                    taskDescriptorStorage);
        }
    }
}
