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

import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Coordinator-wide JMX stats for CTE materialization, exported under
 * {@code trino.cte:name=CteMaterializationStats} and auto-scraped by the JMX exporter. Designed to answer
 * "how well is the feature working": adoption (how much is being materialized), health (how often it falls
 * back or finds nowhere to put scratch), and cost (how long the scratch CTAS take, sweeper activity).
 * <p>
 * All counters are monotonic {@link CounterStat}s (Prometheus reads {@code *.TotalCount} as a counter and
 * the rate windows as gauges). Incremented from {@code SqlQueryExecution.maybeMaterializeCtes}, the
 * {@link CteMaterializationOrchestrator}, and the {@link CteScratchSweeper}.
 */
public class CteMaterializationStats
{
    // adoption / volume
    private final CounterStat queriesMaterialized = new CounterStat();
    private final CounterStat ctesMaterialized = new CounterStat();
    private final CounterStat estimatedRowsSaved = new CounterStat();

    // health
    private final CounterStat materializationFallbacks = new CounterStat();
    private final CounterStat ctesInlinedNoLocation = new CounterStat();

    // scratch table lifecycle
    private final CounterStat scratchTablesCreated = new CounterStat();
    private final CounterStat scratchTablesDropped = new CounterStat();
    private final CounterStat scratchDropFailures = new CounterStat();
    private final TimeStat scratchCtasTime = new TimeStat(MILLISECONDS);

    // orphan sweeper
    private final CounterStat orphanScratchDropped = new CounterStat();
    private final CounterStat sweepRuns = new CounterStat();
    private final CounterStat sweepErrors = new CounterStat();

    /** A query materialized {@code cteCount} CTE(s); {@code estimatedRowsSavedForQuery} is the summed
     * {@code (references - 1) * source-rows} over the materialized CTEs whose source size was known (0 if none). */
    public void queryMaterialized(long cteCount, long estimatedRowsSavedForQuery)
    {
        queriesMaterialized.update(1);
        ctesMaterialized.update(cteCount);
        if (estimatedRowsSavedForQuery > 0) {
            estimatedRowsSaved.update(estimatedRowsSavedForQuery);
        }
    }

    /** Materialization was attempted but failed (CTAS or re-analysis) and the query fell back to inlining. */
    public void materializationFallback()
    {
        materializationFallbacks.update(1);
    }

    /** An eligible CTE was inlined because no scratch location could be determined for it. */
    public void cteInlinedNoLocation()
    {
        ctesInlinedNoLocation.update(1);
    }

    /** A scratch CTAS committed successfully in {@code elapsedNanos}. */
    public void scratchCreated(long elapsedNanos)
    {
        scratchTablesCreated.update(1);
        scratchCtasTime.addNanos(elapsedNanos);
    }

    public void scratchDropped()
    {
        scratchTablesDropped.update(1);
    }

    public void scratchDropFailed()
    {
        scratchDropFailures.update(1);
    }

    public void orphanDropped()
    {
        orphanScratchDropped.update(1);
    }

    public void sweepCompleted()
    {
        sweepRuns.update(1);
    }

    public void sweepFailed()
    {
        sweepErrors.update(1);
    }

    @Managed
    @Nested
    public CounterStat getQueriesMaterialized()
    {
        return queriesMaterialized;
    }

    @Managed
    @Nested
    public CounterStat getCtesMaterialized()
    {
        return ctesMaterialized;
    }

    @Managed
    @Nested
    public CounterStat getEstimatedRowsSaved()
    {
        return estimatedRowsSaved;
    }

    @Managed
    @Nested
    public CounterStat getMaterializationFallbacks()
    {
        return materializationFallbacks;
    }

    @Managed
    @Nested
    public CounterStat getCtesInlinedNoLocation()
    {
        return ctesInlinedNoLocation;
    }

    @Managed
    @Nested
    public CounterStat getScratchTablesCreated()
    {
        return scratchTablesCreated;
    }

    @Managed
    @Nested
    public CounterStat getScratchTablesDropped()
    {
        return scratchTablesDropped;
    }

    @Managed
    @Nested
    public CounterStat getScratchDropFailures()
    {
        return scratchDropFailures;
    }

    @Managed
    @Nested
    public TimeStat getScratchCtasTime()
    {
        return scratchCtasTime;
    }

    @Managed
    @Nested
    public CounterStat getOrphanScratchDropped()
    {
        return orphanScratchDropped;
    }

    @Managed
    @Nested
    public CounterStat getSweepRuns()
    {
        return sweepRuns;
    }

    @Managed
    @Nested
    public CounterStat getSweepErrors()
    {
        return sweepErrors;
    }
}
