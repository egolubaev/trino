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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestCteMaterializationConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CteMaterializationConfig.class)
                .setStrategy(CteMaterializationStrategy.NONE)
                .setMinReferences(2)
                .setMinScanSavings(1_000_000)
                .setOrphanSweepSchemas(ImmutableList.of())
                .setOrphanSweepInterval(new Duration(10, MINUTES))
                .setOrphanSweepMinAge(new Duration(1, HOURS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("cte-materialization.strategy", "HEURISTIC")
                .put("cte-materialization.min-references", "3")
                .put("cte-materialization.min-scan-savings", "50000000")
                .put("cte-materialization.orphan-sweep.schemas", "lakehouse.scratch,hive.tmp")
                .put("cte-materialization.orphan-sweep.interval", "5m")
                .put("cte-materialization.orphan-sweep.min-age", "30m")
                .buildOrThrow();

        CteMaterializationConfig expected = new CteMaterializationConfig()
                .setStrategy(CteMaterializationStrategy.HEURISTIC)
                .setMinReferences(3)
                .setMinScanSavings(50_000_000)
                .setOrphanSweepSchemas(ImmutableList.of("lakehouse.scratch", "hive.tmp"))
                .setOrphanSweepInterval(new Duration(5, MINUTES))
                .setOrphanSweepMinAge(new Duration(30, MINUTES));

        assertFullMapping(properties, expected);
    }
}
