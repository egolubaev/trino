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
import static io.trino.SystemSessionProperties.CTE_MATERIALIZATION_ENABLED;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * End-to-end test of automatic CTE materialization (M1): a real {@code WITH x AS (...) SELECT ... x ... x}
 * query, with {@code cte_materialization_enabled=true}, is transparently rewritten by the engine to
 * materialize x into a per-query scratch table and read it, against an in-JVM Iceberg REST catalog.
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
                .setSystemProperty(CTE_MATERIALIZATION_ENABLED, "true")
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
                .setSystemProperty(CTE_MATERIALIZATION_ENABLED, "true")
                .build();

        assertThat(runner.execute(enabled, query).getOnlyValue()).isEqualTo(3L);

        boolean materialized = runner.getCoordinator().getQueryManager().getQueries().stream()
                .map(BasicQueryInfo::getQuery)
                .anyMatch(sql -> sql.toLowerCase(ENGLISH).contains(".cte_y_"));
        assertThat(materialized)
                .as("a single-reference CTE must not be materialized")
                .isFalse();
    }
}
