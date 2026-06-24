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
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.util.List;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Coordinator configuration for the CTE-materialization orphan-scratch sweeper. Per-query scratch tables
 * are normally dropped by a terminal-state listener, but a coordinator crash between the scratch CTAS and
 * its DROP leaks the table. When {@code cte-materialization.orphan-sweep.schemas} lists one or more
 * {@code catalog.schema} locations, a background task periodically drops {@code cte_*} tables there whose
 * originating query is no longer known to the coordinator and whose query-id timestamp is older than the
 * minimum age. Empty (the default) disables the sweeper.
 */
public class CteMaterializationConfig
{
    private List<String> orphanSweepSchemas = ImmutableList.of();
    private Duration orphanSweepInterval = new Duration(10, MINUTES);
    private Duration orphanSweepMinAge = new Duration(1, HOURS);

    public List<String> getOrphanSweepSchemas()
    {
        return orphanSweepSchemas;
    }

    @Config("cte-materialization.orphan-sweep.schemas")
    @ConfigDescription("Comma-separated catalog.schema locations to sweep for leaked CTE scratch tables (empty disables the sweeper)")
    public CteMaterializationConfig setOrphanSweepSchemas(String schemas)
    {
        this.orphanSweepSchemas = (schemas == null || schemas.isBlank())
                ? ImmutableList.of()
                : ImmutableList.copyOf(Splitter.on(',').trimResults().omitEmptyStrings().split(schemas));
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
    @ConfigDescription("Minimum age (derived from the query-id timestamp) before a leftover scratch table is treated as an orphan; must exceed the longest expected query duration")
    public CteMaterializationConfig setOrphanSweepMinAge(Duration orphanSweepMinAge)
    {
        this.orphanSweepMinAge = orphanSweepMinAge;
        return this;
    }
}
