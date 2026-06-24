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
import com.google.inject.Key;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.Session;
import io.trino.cte.CteMaterializationOrchestrator;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Spike: proves the staged-CTE-materialization orchestrator mechanism end-to-end against an in-JVM
 * Iceberg REST catalog (DelegatingRestSessionCatalog over a JDBC backend). It de-risks the make-or-break
 * facts of the staged design:
 * <ul>
 *   <li>P2/P3 (PRIMARY): an internal {@code CREATE TABLE AS} committed in its own autocommit transaction
 *       is immediately VISIBLE to a subsequent, separate query (no stale metadata cache hides it).</li>
 *   <li>P2: the staged path (read the scratch table twice) returns results identical to the inlined CTE.</li>
 *   <li>P3: {@code DROP TABLE} removes the scratch table (purge is delegated to the REST server).</li>
 * </ul>
 * The "single scan" property (P1) is structural here: the main query references only the scratch table,
 * so the source relation is scanned exactly once — inside the CTAS. The deadlock concern (P4, blocking
 * inside a live parent query) is deferred to M1 where the orchestrator is wired into doPlanQuery.
 */
@TestInstance(PER_CLASS)
public class TestCteMaterializationOrchestrator
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "cte_spike";

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

    private CteMaterializationOrchestrator orchestrator()
    {
        // fetch the bound singleton (also validates the Guice wiring / cycle break)
        return getQueryRunner().getCoordinator().getInstance(Key.get(CteMaterializationOrchestrator.class));
    }

    @Test
    public void testStagedMaterializationMatchesInlinedCte()
    {
        QueryRunner runner = getQueryRunner();
        Session session = runner.getDefaultSession();
        CteMaterializationOrchestrator orchestrator = orchestrator();

        runner.execute("CREATE SCHEMA IF NOT EXISTS iceberg." + SCHEMA);
        runner.execute("CREATE TABLE iceberg." + SCHEMA + ".cte_src AS " +
                "SELECT * FROM (VALUES (1, 10), (1, 20), (2, 30), (3, 40), (3, 50), (3, 60)) t(k, v)");

        String cteBody = "SELECT k, sum(v) AS s, count(*) AS c FROM iceberg." + SCHEMA + ".cte_src GROUP BY k";

        // Baseline: CTE inlined and referenced twice (today this scans cte_src twice).
        MaterializedResult inlined = runner.execute(
                "WITH x AS (" + cteBody + ") " +
                "SELECT a.k, a.s, b.c FROM x a JOIN x b ON a.k = b.k ORDER BY a.k");

        // Stage: materialize the CTE into a per-query scratch table (separate autocommit transaction).
        String scratch = "iceberg." + SCHEMA + ".cte_scratch_" + randomNameSuffix();
        orchestrator.materialize(session, scratch, cteBody);

        // P3/P2 PRIMARY: the just-committed scratch table is visible to a fresh, separate query.
        assertThat(runner.execute("SELECT count(*) FROM " + scratch).getOnlyValue())
                .isEqualTo(3L);

        // Staged path: the main query reads the scratch table instead of re-scanning the source.
        MaterializedResult staged = runner.execute(
                "SELECT a.k, a.s, b.c FROM " + scratch + " a JOIN " + scratch + " b ON a.k = b.k ORDER BY a.k");

        // P2: bit-identical to the inlined CTE result.
        assertThat(staged.getMaterializedRows()).isEqualTo(inlined.getMaterializedRows());

        // Cleanup: DROP purges under the REST catalog.
        orchestrator.cleanup(session, scratch);

        // P3: the scratch table is gone.
        String scratchTableName = scratch.substring(scratch.lastIndexOf('.') + 1);
        assertThat(runner.execute("SHOW TABLES FROM iceberg." + SCHEMA).getMaterializedRows().stream()
                .map(row -> (String) row.getField(0)))
                .doesNotContain(scratchTableName);
    }
}
