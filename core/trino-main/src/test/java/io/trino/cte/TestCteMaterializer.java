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

import io.trino.cte.CteMaterializer.CteCandidate;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure-AST unit tests for CTE materialization detection and rewrite (no server / no catalog).
 */
public class TestCteMaterializer
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void detectsMultiplyReferencedCte()
    {
        List<CteCandidate> candidates = CteMaterializer.findCandidates(parse(
                "WITH x AS (SELECT a, sum(b) AS s FROM t GROUP BY a) " +
                "SELECT * FROM x p JOIN x q ON p.a = q.a"));
        assertThat(candidates).extracting(CteCandidate::name).containsExactly("x");
    }

    @Test
    public void ignoresSingleReferenceCte()
    {
        assertThat(CteMaterializer.findCandidates(parse(
                "WITH x AS (SELECT a FROM t) SELECT * FROM x")))
                .isEmpty();
    }

    @Test
    public void ignoresRecursiveCte()
    {
        assertThat(CteMaterializer.findCandidates(parse(
                "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM x WHERE n < 5) " +
                "SELECT * FROM x a JOIN x b ON a.n = b.n")))
                .isEmpty();
    }

    @Test
    public void ignoresNonDeterministicCte()
    {
        assertThat(CteMaterializer.findCandidates(parse(
                "WITH x AS (SELECT a, rand() AS r FROM t) SELECT * FROM x p JOIN x q ON p.a = q.a")))
                .as("CTE with rand() must not be materialized")
                .isEmpty();
        assertThat(CteMaterializer.findCandidates(parse(
                "WITH x AS (SELECT a, now() AS ts FROM t) SELECT * FROM x p JOIN x q ON p.a = q.a")))
                .as("CTE with now() must not be materialized")
                .isEmpty();
        assertThat(CteMaterializer.findCandidates(parse(
                "WITH x AS (SELECT a, current_timestamp AS ts FROM t) SELECT * FROM x p JOIN x q ON p.a = q.a")))
                .as("CTE with current_timestamp must not be materialized")
                .isEmpty();
    }

    @Test
    public void ignoresCteWhoseBodyReferencesAnotherCte()
    {
        // y references CTE x (not standalone) -> y excluded; x referenced only once (inside y) -> excluded
        assertThat(CteMaterializer.findCandidates(parse(
                "WITH x AS (SELECT a FROM t), y AS (SELECT a FROM x) " +
                "SELECT * FROM y p JOIN y q ON p.a = q.a")))
                .isEmpty();
    }

    @Test
    public void ignoresCteWithExplicitColumnAliases()
    {
        assertThat(CteMaterializer.findCandidates(parse(
                "WITH x(c) AS (SELECT a FROM t) SELECT * FROM x p JOIN x q ON p.c = q.c")))
                .isEmpty();
    }

    @Test
    public void rewriteSwapsBodyToScratchScan()
    {
        Statement original = parse(
                "WITH x AS (SELECT a, sum(b) AS s FROM t GROUP BY a) " +
                "SELECT * FROM x p JOIN x q ON p.a = q.a");
        Statement rewritten = CteMaterializer.rewrite(original, Map.of("x", "cat.sch.scratch_x"), SQL_PARSER);
        String sql = SqlFormatter.formatSql(rewritten);
        // the CTE body is now a scan of the scratch table, and the original aggregation is gone from the WITH
        assertThat(sql).contains("scratch_x");
        assertThat(sql).doesNotContain("GROUP BY");
    }

    private static Statement parse(String sql)
    {
        return SQL_PARSER.createStatement(sql);
    }
}
