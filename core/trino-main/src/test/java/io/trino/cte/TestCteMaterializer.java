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
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

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
    public void reportsReferenceCount()
    {
        // x referenced twice, y referenced three times -> both eligible, counts reported for HEURISTIC gating
        List<CteCandidate> candidates = CteMaterializer.findCandidates(parse(
                "WITH x AS (SELECT a FROM t), y AS (SELECT a FROM u) " +
                "SELECT * FROM x p JOIN x q ON p.a = q.a " +
                "JOIN y r ON r.a = p.a JOIN y s ON s.a = q.a JOIN y v ON v.a = r.a"));
        assertThat(candidates)
                .extracting(CteCandidate::name, CteCandidate::referenceCount)
                .containsExactlyInAnyOrder(tuple("x", 2), tuple("y", 3));
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
    public void materializesCteThatReferencesAnotherCte()
    {
        // y references sibling x and is itself referenced twice -> y is materialized (x inlined into its CTAS);
        // x is referenced only once (inside y) -> x stays inline
        List<CteCandidate> candidates = CteMaterializer.findCandidates(parse(
                "WITH x AS (SELECT a FROM t), y AS (SELECT a FROM x) " +
                "SELECT * FROM y p JOIN y q ON p.a = q.a"));
        assertThat(candidates)
                .extracting(CteCandidate::name)
                .containsExactly("y");
    }

    @Test
    public void ignoresCteWithExplicitColumnAliases()
    {
        assertThat(CteMaterializer.findCandidates(parse(
                "WITH x(c) AS (SELECT a FROM t) SELECT * FROM x p JOIN x q ON p.c = q.c")))
                .isEmpty();
    }

    @Test
    public void linearChainMaterializesOnlyMultiplyReferencedCte()
    {
        // a is referenced once (inside b); b is referenced twice -> only b is a candidate, a is inlined into b's CTAS
        List<CteCandidate> candidates = CteMaterializer.findCandidates(parse(
                "WITH a AS (SELECT k FROM t), b AS (SELECT k FROM a) " +
                "SELECT * FROM b x JOIN b y ON x.k = y.k"));
        assertThat(candidates)
                .extracting(CteCandidate::name, CteCandidate::referenceCount)
                .containsExactly(tuple("b", 2));
    }

    @Test
    public void sharedDependencyInChainIsMaterialized()
    {
        // a is referenced twice (by b and c); b and c are each referenced twice -> all three are candidates
        List<CteCandidate> candidates = CteMaterializer.findCandidates(parse(
                "WITH a AS (SELECT k FROM t), b AS (SELECT k FROM a), c AS (SELECT k FROM a) " +
                "SELECT * FROM b j1 JOIN b j2 ON j1.k = j2.k JOIN c k1 ON k1.k = j1.k JOIN c k2 ON k2.k = k1.k"));
        assertThat(candidates)
                .extracting(CteCandidate::name, CteCandidate::referenceCount)
                .containsExactly(tuple("a", 2), tuple("b", 2), tuple("c", 2));
    }

    @Test
    public void ignoresChainThatIsTransitivelyNonDeterministic()
    {
        // b's body is deterministic on its own, but it depends on a which uses rand() -> b must not be materialized
        assertThat(CteMaterializer.findCandidates(parse(
                "WITH a AS (SELECT rand() AS r FROM t), b AS (SELECT r FROM a) " +
                "SELECT * FROM b x JOIN b y ON x.r = y.r")))
                .as("a CTE that transitively depends on rand() must not be materialized")
                .isEmpty();
    }

    @Test
    public void ignoresCteWithDependencyAndNestedWith()
    {
        // b depends on sibling a AND has its own nested WITH -> excluded (cannot wrap body in a dependency WITH)
        assertThat(CteMaterializer.findCandidates(parse(
                "WITH a AS (SELECT k FROM t), b AS (WITH w AS (SELECT k FROM a) SELECT k FROM w) " +
                "SELECT * FROM b x JOIN b y ON x.k = y.k")))
                .extracting(CteCandidate::name)
                .doesNotContain("b");
    }

    @Test
    public void buildScratchSourceInlinesUnmaterializedDependency()
    {
        Statement stmt = parse(
                "WITH a AS (SELECT k FROM t), b AS (SELECT k FROM a) " +
                "SELECT * FROM b x JOIN b y ON x.k = y.k");
        // no scratch tables yet -> a is inlined into b's CTAS via a WITH clause
        String source = normalize(CteMaterializer.buildScratchSource(stmt, "b", Map.of(), SQL_PARSER));
        assertThat(source).contains("WITH a AS");
        assertThat(source).contains("FROM t");
    }

    @Test
    public void buildScratchSourceRedefinesMaterializedDependencyAsScratchScan()
    {
        Statement stmt = parse(
                "WITH a AS (SELECT k FROM t), b AS (SELECT k FROM a) " +
                "SELECT * FROM b x JOIN b y ON x.k = y.k");
        // a already materialized -> b's CTAS reads a's scratch table, not the original source t
        String source = normalize(CteMaterializer.buildScratchSource(stmt, "b", Map.of("a", "cat.sch.scratch_a"), SQL_PARSER));
        assertThat(source).contains("scratch_a");
        assertThat(source).doesNotContain("FROM t");
    }

    @Test
    public void buildScratchSourceForIndependentCteIsJustItsBody()
    {
        Statement stmt = parse(
                "WITH a AS (SELECT k FROM t), b AS (SELECT k FROM a) " +
                "SELECT * FROM b x JOIN b y ON x.k = y.k");
        // a has no sibling dependencies -> its CTAS source is just its body, no wrapping WITH
        String source = normalize(CteMaterializer.buildScratchSource(stmt, "a", Map.of(), SQL_PARSER));
        assertThat(source).doesNotContainIgnoringCase("WITH");
        assertThat(source).contains("FROM t");
    }

    @Test
    public void sourceTablesForStandaloneCteListsBaseTables()
    {
        Statement stmt = parse("WITH a AS (SELECT k FROM cat.sch.t) SELECT * FROM a x JOIN a y ON x.k = y.k");
        Optional<List<QualifiedName>> tables = CteMaterializer.sourceTablesForCostEstimate(stmt, "a");
        assertThat(tables).isPresent();
        assertThat(tables.get()).extracting(QualifiedName::toString).containsExactly("cat.sch.t");
    }

    @Test
    public void sourceTablesForDependentCteIsUnknown()
    {
        // b depends on sibling a -> its scanned volume is not cheaply sizable -> unknown (cost gate won't prune)
        Statement stmt = parse("WITH a AS (SELECT k FROM t), b AS (SELECT k FROM a) SELECT * FROM b x JOIN b y ON x.k = y.k");
        assertThat(CteMaterializer.sourceTablesForCostEstimate(stmt, "b")).isEmpty();
    }

    @Test
    public void firstQualifiedTableFindsThreePartName()
    {
        Statement stmt = parse("WITH cm AS (SELECT k FROM lakehouse.bench.src) SELECT * FROM cm a JOIN cm b ON a.k = b.k");
        assertThat(CteMaterializer.firstQualifiedTable(stmt))
                .map(QualifiedName::toString)
                .contains("lakehouse.bench.src");
    }

    @Test
    public void firstQualifiedTableEmptyWhenNoneQualified()
    {
        Statement stmt = parse("WITH cm AS (SELECT k FROM src) SELECT * FROM cm a JOIN cm b ON a.k = b.k");
        assertThat(CteMaterializer.firstQualifiedTable(stmt)).isEmpty();
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

    @Test
    public void firstQualifiedTableForCteUsesCteOwnSource()
    {
        // x reads iceberg, y reads clickhouse -> each CTE's scratch should follow its own source catalog
        Statement statement = parse(
                "WITH x AS (SELECT a, sum(b) s FROM iceberg.bench.t GROUP BY a), " +
                "     y AS (SELECT a, count(*) c FROM clickhouse.raw.u GROUP BY a) " +
                "SELECT * FROM x j1 JOIN x j2 ON j1.a = j2.a JOIN y k1 ON k1.a = j1.a JOIN y k2 ON k2.a = j2.a");
        assertThat(CteMaterializer.firstQualifiedTableForCte(statement, "x"))
                .map(QualifiedName::toString).contains("iceberg.bench.t");
        assertThat(CteMaterializer.firstQualifiedTableForCte(statement, "y"))
                .map(QualifiedName::toString).contains("clickhouse.raw.u");
    }

    @Test
    public void firstQualifiedTableForCteFollowsDependencyClosure()
    {
        // y has no base table of its own; it reads dependency x, whose source catalog must be discovered
        Statement statement = parse(
                "WITH x AS (SELECT a, b FROM clickhouse.raw.u), " +
                "     y AS (SELECT a, sum(b) s FROM x GROUP BY a) " +
                "SELECT * FROM y p JOIN y q ON p.a = q.a");
        assertThat(CteMaterializer.firstQualifiedTableForCte(statement, "y"))
                .map(QualifiedName::toString).contains("clickhouse.raw.u");
    }

    @Test
    public void firstQualifiedTableForCteEmptyWhenUnqualified()
    {
        // unqualified source resolves through the session default -> no catalog derivable from the AST
        Statement statement = parse(
                "WITH x AS (SELECT a, sum(b) s FROM t GROUP BY a) SELECT * FROM x p JOIN x q ON p.a = q.a");
        assertThat(CteMaterializer.firstQualifiedTableForCte(statement, "x")).isEmpty();
    }

    private static Statement parse(String sql)
    {
        return SQL_PARSER.createStatement(sql);
    }

    private static String normalize(String sql)
    {
        // collapse SqlFormatter's line breaks/indentation so substring assertions are layout-independent
        return sql.replaceAll("\\s+", " ").trim();
    }
}
