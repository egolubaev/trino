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

import io.trino.execution.QueryState;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the orphan-scratch sweeper's pure decision helpers (no catalog / no server).
 */
public class TestCteScratchSweeper
{
    private static final Instant NOW = Instant.parse("2026-06-24T10:00:00Z");
    private static final Duration MIN_AGE = Duration.ofHours(1);
    private static final Function<QueryId, Optional<QueryState>> UNKNOWN = queryId -> Optional.empty();
    private static final Function<QueryId, Optional<QueryState>> RUNNING = queryId -> Optional.of(QueryState.RUNNING);
    private static final Function<QueryId, Optional<QueryState>> FINISHED = queryId -> Optional.of(QueryState.FINISHED);

    @Test
    public void extractsTrailingQueryId()
    {
        assertThat(CteScratchSweeper.extractQueryId("cte_cm_20260624_074852_00000_cpqed"))
                .contains("20260624_074852_00000_cpqed");
        // a CTE name containing underscores and digits must not confuse extraction
        assertThat(CteScratchSweeper.extractQueryId("cte_my_2nd_table_20260624_074852_00012_abcde"))
                .contains("20260624_074852_00012_abcde");
        assertThat(CteScratchSweeper.extractQueryId("cte_x_not_a_query_id")).isEmpty();
        assertThat(CteScratchSweeper.extractQueryId("orders")).isEmpty();
    }

    @Test
    public void parsesUtcTimestampFromQueryId()
    {
        assertThat(CteScratchSweeper.queryIdTimestamp("20260624_074852_00000_cpqed"))
                .contains(Instant.parse("2026-06-24T07:48:52Z"));
    }

    @Test
    public void oldOrphanWithUnknownQueryIsSwept()
    {
        // timestamp 07:48:52Z, now 10:00:00Z -> ~2h11m old > 1h, and unknown -> orphan (e.g. lost to a crash)
        assertThat(CteScratchSweeper.isSweepableOrphan("cte_cm_20260624_074852_00000_cpqed", NOW, MIN_AGE, UNKNOWN))
                .isTrue();
    }

    @Test
    public void runningQueryIsNeverSweptEvenWhenOld()
    {
        // an old timestamp but the query is still RUNNING (e.g. a multi-hour query) -> must be kept
        assertThat(CteScratchSweeper.isSweepableOrphan("cte_cm_20260624_074852_00000_cpqed", NOW, MIN_AGE, RUNNING))
                .as("a still-running query's scratch must never be dropped regardless of age")
                .isFalse();
    }

    @Test
    public void finishedButLeakedScratchIsSwept()
    {
        // query FINISHED (terminal) but its scratch leaked (cleanup listener failed) and is old enough -> swept
        assertThat(CteScratchSweeper.isSweepableOrphan("cte_cm_20260624_074852_00000_cpqed", NOW, MIN_AGE, FINISHED))
                .as("a leaked scratch from a finished query should be reclaimed without waiting for history expiry")
                .isTrue();
    }

    @Test
    public void recentOrphanIsNotYetSwept()
    {
        // timestamp 09:59:59Z, now 10:00:00Z -> ~1s old < 1h -> too recent to sweep even though unknown
        assertThat(CteScratchSweeper.isSweepableOrphan("cte_cm_20260624_095959_00000_cpqed", NOW, MIN_AGE, UNKNOWN))
                .as("a recent scratch table must be left alone (might belong to another coordinator)")
                .isFalse();
    }

    @Test
    public void nonScratchTablesAreIgnored()
    {
        assertThat(CteScratchSweeper.isSweepableOrphan("orders", NOW, MIN_AGE, UNKNOWN)).isFalse();
        assertThat(CteScratchSweeper.isSweepableOrphan("lineitem_20260624_074852_00000_cpqed", NOW, MIN_AGE, UNKNOWN))
                .as("only names starting with cte_ are scratch tables")
                .isFalse();
        // a cte_-prefixed table without a parseable query id is left alone
        assertThat(CteScratchSweeper.isSweepableOrphan("cte_handmade_table", NOW, MIN_AGE, UNKNOWN)).isFalse();
    }
}
