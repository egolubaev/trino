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

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.List;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Cluster-wide configuration for CTE materialization.
 * <p>
 * The {@code strategy} / {@code min-references} / {@code min-scan-savings} properties set the cluster
 * defaults for the matching session properties ({@code cte_materialization_strategy},
 * {@code cte_materialization_min_references}, {@code cte_materialization_min_scan_savings}); a
 * {@code SET SESSION} still overrides them per query.
 * <p>
 * The {@code orphan-sweep.*} properties configure the background sweeper that reclaims scratch tables
 * leaked by a coordinator crash or a failed cleanup (see {@link CteScratchSweeper}).
 */
public class CteMaterializationConfig
{
    private CteMaterializationStrategy strategy = CteMaterializationStrategy.NONE;
    private int minReferences = 2;
    private long minScanSavings = 1_000_000;

    private List<String> orphanSweepSchemas = ImmutableList.of();
    private Duration orphanSweepInterval = new Duration(10, MINUTES);
    private Duration orphanSweepMinAge = new Duration(1, HOURS);

    @NotNull
    public CteMaterializationStrategy getStrategy()
    {
        return strategy;
    }

    @Config("cte-materialization.strategy")
    @ConfigDescription("Cluster default for cte_materialization_strategy: NONE (inline), ALL, or HEURISTIC")
    public CteMaterializationConfig setStrategy(CteMaterializationStrategy strategy)
    {
        this.strategy = strategy;
        return this;
    }

    @Min(2)
    public int getMinReferences()
    {
        return minReferences;
    }

    @Config("cte-materialization.min-references")
    @ConfigDescription("Cluster default for cte_materialization_min_references (HEURISTIC reference-count threshold)")
    public CteMaterializationConfig setMinReferences(int minReferences)
    {
        this.minReferences = minReferences;
        return this;
    }

    @Min(0)
    public long getMinScanSavings()
    {
        return minScanSavings;
    }

    @Config("cte-materialization.min-scan-savings")
    @ConfigDescription("Cluster default for cte_materialization_min_scan_savings (HEURISTIC minimum estimated repeated-scan savings, in rows)")
    public CteMaterializationConfig setMinScanSavings(long minScanSavings)
    {
        this.minScanSavings = minScanSavings;
        return this;
    }

    public List<String> getOrphanSweepSchemas()
    {
        return orphanSweepSchemas;
    }

    @Config("cte-materialization.orphan-sweep.schemas")
    @ConfigDescription("Comma-separated catalog.schema locations to sweep for leaked CTE scratch tables (empty disables the sweeper)")
    public CteMaterializationConfig setOrphanSweepSchemas(List<String> orphanSweepSchemas)
    {
        this.orphanSweepSchemas = ImmutableList.copyOf(orphanSweepSchemas);
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getOrphanSweepInterval()
    {
        return orphanSweepInterval;
    }

    @Config("cte-materialization.orphan-sweep.interval")
    @ConfigDescription("How often the orphan-scratch sweeper runs")
    public CteMaterializationConfig setOrphanSweepInterval(Duration orphanSweepInterval)
    {
        this.orphanSweepInterval = orphanSweepInterval;
        return this;
    }

    @NotNull
    public Duration getOrphanSweepMinAge()
    {
        return orphanSweepMinAge;
    }

    @Config("cte-materialization.orphan-sweep.min-age")
    @ConfigDescription("Minimum age (from the query-id timestamp) before a leftover scratch table is treated as an orphan; must exceed the longest expected query duration")
    public CteMaterializationConfig setOrphanSweepMinAge(Duration orphanSweepMinAge)
    {
        this.orphanSweepMinAge = orphanSweepMinAge;
        return this;
    }
}
