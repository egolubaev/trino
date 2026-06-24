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
package io.trino.plugin.iceberg.cte;

import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.Session;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.server.BasicQueryInfo;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.SystemSessionProperties.CTE_MATERIALIZATION_MIN_REFERENCES;
import static io.trino.SystemSessionProperties.CTE_MATERIALIZATION_MIN_SCAN_SAVINGS;
import static io.trino.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * End-to-end test of automatic CTE materialization: a real {@code WITH x AS (...) SELECT ... x ... x}
 * query, with {@code cte_materialization_strategy='ALL'}, is transparently rewritten by the engine to
 * materialize x into a per-query scratch table and read it, against an in-JVM Iceberg REST catalog.
 * The HEURISTIC strategy's {@code cte_materialization_min_references} gate is also exercised.
 * <p>
 * This exercises the production transaction-scoping path: the parent query runs in its own autocommit
 * transaction while the child scratch CTAS commits independently and is visible to the rewritten main query.
 */
@TestInstance(PER_CLASS)
public class TestCteMaterializationEndToEnd
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "cte_e2e";

    private Path warehouseLocation;
    private JdbcCatalog backend;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warehouseLocation = Files.createTempDirectory(null);
        closeAfterClass(() -> deleteRecursively(warehouseLocation, ALLOW_INSECURE));

        backend = closeAfterClass((JdbcCatalog) backendCatalog(warehouseLocation));

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        // force loopback: getBaseUrl() may advertise a (VPN/LAN) interface IP that isn't self-reachable
        URI baseUrl = testServer.getBaseUrl();
        String restUri = "http://127.0.0.1:" + baseUrl.getPort() + baseUrl.getPath();

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation))
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "rest")
                        .put("iceberg.rest-catalog.uri", restUri)
                        .buildOrThrow())
                .build();
    }

    @BeforeAll
    public void createSchema()
    {
        // create once: the in-JVM REST backend throws on a second CREATE SCHEMA IF NOT EXISTS
        getQueryRunner().execute("CREATE SCHEMA IF NOT EXISTS iceberg." + SCHEMA);
    }

    @Test
    public void testAutoMaterializationPreservesResults()
    {
        QueryRunner runner = getQueryRunner();
        runner.execute("DROP TABLE IF EXISTS iceberg." + SCHEMA + ".src");
        runner.execute("CREATE TABLE iceberg." + SCHEMA + ".src AS " +
                "SELECT * FROM (VALUES (1, 10), (1, 20), (2, 30), (3, 40), (3, 50), (3, 60)) t(k, v)");

        // x is referenced twice; without materialization the source is scanned twice
        @Language("SQL") String query =
                "WITH x AS (SELECT k, sum(v) AS s, count(*) AS c FROM iceberg." + SCHEMA + ".src GROUP BY k) " +
                "SELECT a.k, a.s, b.c FROM x a JOIN x b ON a.k = b.k ORDER BY a.k";

        Session disabled = getSession();
        Session enabled = Session.builder(disabled)
                .setSchema(SCHEMA)                 // scratch tables land in the session's default schema
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .build();

        MaterializedResult expected = runner.execute(disabled, query);
        MaterializedResult actual = runner.execute(enabled, query);

        // correctness: materialization must not change results
        assertThat(actual.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());

        // trigger proof: a child scratch CTAS for CTE "x" was submitted
        boolean materialized = runner.getCoordinator().getQueryManager().getQueries().stream()
                .map(BasicQueryInfo::getQuery)
                .anyMatch(sql -> sql.toUpperCase(ENGLISH).startsWith("CREATE TABLE")
                        && sql.toLowerCase(ENGLISH).contains(".cte_x_"));
        assertThat(materialized)
                .as("a scratch CTAS for CTE x should have been submitted")
                .isTrue();
    }

    @Test
    public void testSingleReferenceCteIsNotMaterialized()
    {
        QueryRunner runner = getQueryRunner();
        runner.execute("DROP TABLE IF EXISTS iceberg." + SCHEMA + ".src1");
        runner.execute("CREATE TABLE iceberg." + SCHEMA + ".src1 AS SELECT * FROM (VALUES 1, 2, 3) t(k)");

        // single reference -> not worth materializing
        @Language("SQL") String query =
                "WITH y AS (SELECT k FROM iceberg." + SCHEMA + ".src1) SELECT count(*) FROM y";
        Session enabled = Session.builder(getSession())
                .setSchema(SCHEMA)
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .build();

        assertThat(runner.execute(enabled, query).getOnlyValue()).isEqualTo(3L);

        boolean materialized = runner.getCoordinator().getQueryManager().getQueries().stream()
                .map(BasicQueryInfo::getQuery)
                .anyMatch(sql -> sql.toLowerCase(ENGLISH).contains(".cte_y_"));
        assertThat(materialized)
                .as("a single-reference CTE must not be materialized")
                .isFalse();
    }

    @Test
    public void testHeuristicRespectsMinReferences()
    {
        QueryRunner runner = getQueryRunner();
        runner.execute("DROP TABLE IF EXISTS iceberg." + SCHEMA + ".src2");
        runner.execute("CREATE TABLE iceberg." + SCHEMA + ".src2 AS " +
                "SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) t(k, v)");

        // z is referenced exactly twice
        @Language("SQL") String query =
                "WITH z AS (SELECT k, v FROM iceberg." + SCHEMA + ".src2) " +
                "SELECT a.k, b.v FROM z a JOIN z b ON a.k = b.k ORDER BY a.k";

        // min_scan_savings=1 isolates the reference-count gate from the cost gate (tiny source)
        // HEURISTIC with threshold 3: a CTE referenced only twice must NOT be materialized
        Session belowThreshold = Session.builder(getSession())
                .setSchema(SCHEMA)
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC")
                .setSystemProperty(CTE_MATERIALIZATION_MIN_REFERENCES, "3")
                .setSystemProperty(CTE_MATERIALIZATION_MIN_SCAN_SAVINGS, "1")
                .build();
        runner.execute(belowThreshold, query);
        assertThat(scratchSubmittedFor(runner, ".cte_z_"))
                .as("HEURISTIC must not materialize a CTE below cte_materialization_min_references")
                .isFalse();

        // HEURISTIC with threshold 2: the same twice-referenced CTE is now materialized
        Session atThreshold = Session.builder(getSession())
                .setSchema(SCHEMA)
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC")
                .setSystemProperty(CTE_MATERIALIZATION_MIN_REFERENCES, "2")
                .setSystemProperty(CTE_MATERIALIZATION_MIN_SCAN_SAVINGS, "1")
                .build();
        runner.execute(atThreshold, query);
        assertThat(scratchSubmittedFor(runner, ".cte_z_"))
                .as("HEURISTIC must materialize a CTE at/above cte_materialization_min_references")
                .isTrue();
    }

    @Test
    public void testHeuristicCostGateUsesTableStatistics()
    {
        QueryRunner runner = getQueryRunner();
        runner.execute("DROP TABLE IF EXISTS iceberg." + SCHEMA + ".src4");
        runner.execute("CREATE TABLE iceberg." + SCHEMA + ".src4 AS " +
                "SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) t(k, v)");

        // w is referenced twice over a 3-row source -> savings = (2-1)*3 = 3 rows
        @Language("SQL") String query =
                "WITH w AS (SELECT k, v FROM iceberg." + SCHEMA + ".src4) " +
                "SELECT a.k, b.v FROM w a JOIN w b ON a.k = b.k ORDER BY a.k";

        // default savings threshold (1,000,000) >> 3 -> cost gate prunes despite reference count being met
        Session highThreshold = Session.builder(getSession())
                .setSchema(SCHEMA)
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC")
                .build();
        runner.execute(highThreshold, query);
        assertThat(scratchSubmittedFor(runner, ".cte_w_"))
                .as("HEURISTIC must not materialize when estimated scan savings are below the threshold")
                .isFalse();

        // lower the threshold below the estimated savings -> now materialized
        Session lowThreshold = Session.builder(getSession())
                .setSchema(SCHEMA)
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC")
                .setSystemProperty(CTE_MATERIALIZATION_MIN_SCAN_SAVINGS, "1")
                .build();
        runner.execute(lowThreshold, query);
        assertThat(scratchSubmittedFor(runner, ".cte_w_"))
                .as("HEURISTIC must materialize when estimated scan savings reach the threshold")
                .isTrue();
    }

    @Test
    public void testMultiCteChainMaterializesSharedDependency()
    {
        QueryRunner runner = getQueryRunner();
        runner.execute("DROP TABLE IF EXISTS iceberg." + SCHEMA + ".src3");
        runner.execute("CREATE TABLE iceberg." + SCHEMA + ".src3 AS " +
                "SELECT * FROM (VALUES (1, 10), (1, 20), (2, 30), (3, 40), (3, 50), (3, 60)) t(k, v)");

        // a is shared by b and c (referenced twice) and b, c are each referenced twice -> all three materialize.
        // b and c's scratch CTAS must read a's scratch table, not re-scan src3.
        @Language("SQL") String query =
                "WITH a AS (SELECT k, v FROM iceberg." + SCHEMA + ".src3), " +
                "     b AS (SELECT k, sum(v) AS s FROM a GROUP BY k), " +
                "     c AS (SELECT k, count(*) AS cnt FROM a GROUP BY k) " +
                "SELECT b1.k, b1.s, c1.cnt " +
                "FROM b b1 JOIN b b2 ON b1.k = b2.k " +
                "JOIN c c1 ON c1.k = b1.k JOIN c c2 ON c2.k = c1.k " +
                "ORDER BY b1.k";

        Session disabled = getSession();
        Session enabled = Session.builder(disabled)
                .setSchema(SCHEMA)
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .build();

        MaterializedResult expected = runner.execute(disabled, query);
        MaterializedResult actual = runner.execute(enabled, query);
        assertThat(actual.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());

        // all three CTEs were materialized
        assertThat(scratchSubmittedFor(runner, ".cte_a_")).as("shared dependency a materialized").isTrue();
        assertThat(scratchSubmittedFor(runner, ".cte_b_")).as("b materialized").isTrue();
        assertThat(scratchSubmittedFor(runner, ".cte_c_")).as("c materialized").isTrue();

        // dependent CTAS for b reads a's scratch table (proves dependency rewrite, not a re-scan of src3)
        boolean bReadsScratchA = runner.getCoordinator().getQueryManager().getQueries().stream()
                .map(BasicQueryInfo::getQuery)
                .map(sql -> sql.toLowerCase(ENGLISH))
                .anyMatch(sql -> sql.startsWith("create table") && sql.contains(".cte_b_") && sql.contains(".cte_a_"));
        assertThat(bReadsScratchA)
                .as("b's scratch CTAS should read a's scratch table, not re-scan the base source")
                .isTrue();
    }

    @Test
    public void testMaterializesWithoutDefaultSchemaUsingSourceLocation()
    {
        QueryRunner runner = getQueryRunner();
        runner.execute("DROP TABLE IF EXISTS iceberg." + SCHEMA + ".srcq");
        runner.execute("CREATE TABLE iceberg." + SCHEMA + ".srcq AS " +
                "SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) t(k, v)");

        // fully-qualified source; session has NO default catalog/schema (mimics a JDBC client without USE)
        @Language("SQL") String query =
                "WITH cm AS (SELECT k, v FROM iceberg." + SCHEMA + ".srcq) " +
                "SELECT a.k, b.v FROM cm a JOIN cm b ON a.k = b.k ORDER BY a.k";

        Session noDefaults = Session.builder(getSession())
                .setCatalog(Optional.empty())
                .setSchema(Optional.empty())
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .build();

        MaterializedResult result = runner.execute(noDefaults, query);
        assertThat(result.getRowCount()).isEqualTo(3);

        // scratch must be created — placed in the source table's catalog.schema (iceberg.<SCHEMA>)
        boolean placedAtSource = runner.getCoordinator().getQueryManager().getQueries().stream()
                .map(BasicQueryInfo::getQuery)
                .map(sql -> sql.toLowerCase(ENGLISH))
                .anyMatch(sql -> sql.startsWith("create table")
                        && sql.contains("iceberg." + SCHEMA.toLowerCase(ENGLISH) + ".cte_cm_"));
        assertThat(placedAtSource)
                .as("with no default schema, scratch should be placed in the source table's catalog.schema")
                .isTrue();
    }

    @Test
    public void testCollidingSanitizedCteNamesGetDistinctScratchTables()
    {
        QueryRunner runner = getQueryRunner();
        runner.execute("DROP TABLE IF EXISTS iceberg." + SCHEMA + ".srcc");
        runner.execute("CREATE TABLE iceberg." + SCHEMA + ".srcc AS " +
                "SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) t(k, v)");

        // "a-b" (quoted, hyphen) and a_b both sanitize to a_b; the candidate index must keep their scratch
        // table names distinct, otherwise the second CTAS would collide ("table exists") and abort the whole
        // materialization back to inlining
        @Language("SQL") String query =
                "WITH \"a-b\" AS (SELECT k, v FROM iceberg." + SCHEMA + ".srcc), " +
                "     a_b AS (SELECT k, sum(v) AS s FROM iceberg." + SCHEMA + ".srcc GROUP BY k) " +
                "SELECT t1.k, u1.s FROM \"a-b\" t1 JOIN \"a-b\" t2 ON t1.k = t2.k " +
                "JOIN a_b u1 ON u1.k = t1.k JOIN a_b u2 ON u2.k = t1.k ORDER BY t1.k";

        Session disabled = getSession();
        Session enabled = Session.builder(disabled)
                .setSchema(SCHEMA)
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .build();

        MaterializedResult expected = runner.execute(disabled, query);
        MaterializedResult actual = runner.execute(enabled, query);
        assertThat(actual.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());

        // both colliding CTEs materialized into distinct scratch tables (cte_a_b_0_* and cte_a_b_1_*)
        long distinctScratchCtas = runner.getCoordinator().getQueryManager().getQueries().stream()
                .map(BasicQueryInfo::getQuery)
                .map(sql -> sql.toLowerCase(ENGLISH))
                .filter(sql -> sql.startsWith("create table") && sql.contains(".cte_a_b_"))
                .distinct()
                .count();
        assertThat(distinctScratchCtas)
                .as("colliding sanitized CTE names must produce two distinct scratch tables")
                .isEqualTo(2);
    }

    private static boolean scratchSubmittedFor(QueryRunner runner, String scratchInfix)
    {
        return runner.getCoordinator().getQueryManager().getQueries().stream()
                .map(BasicQueryInfo::getQuery)
                .anyMatch(sql -> sql.toUpperCase(ENGLISH).startsWith("CREATE TABLE")
                        && sql.toLowerCase(ENGLISH).contains(scratchInfix));
    }
}
