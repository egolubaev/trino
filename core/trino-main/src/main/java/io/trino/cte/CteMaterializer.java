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
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Locale.ENGLISH;

/**
 * Pure-AST detection and rewrite for staged CTE materialization.
 * <p>
 * Detection finds top-level WITH entries that are worth (and safe to) materialize into a per-query
 * scratch table. Rewrite swaps each materialized CTE's body for {@code SELECT * FROM <scratch>},
 * leaving every reference to the CTE untouched — re-analysis re-resolves them to the scratch scan and
 * predicate/projection pushdown still specializes per consumer.
 * <p>
 * SPIKE/M1 gates (conservative — anything not matching falls back to normal inlining):
 * <ul>
 *   <li>statement is a {@link Query} with a non-recursive {@link With};</li>
 *   <li>the CTE is referenced at least twice;</li>
 *   <li>the CTE has no explicit column-alias list (kept simple for now);</li>
 *   <li>the CTE body references no other CTE (so it is a standalone CTAS);</li>
 *   <li>the CTE body is deterministic (no rand/uuid/now/shuffle and no CURRENT_* contextual values).</li>
 * </ul>
 */
public final class CteMaterializer
{
    // Nondeterministic / non-repeatable function names (lowercased, last name part).
    private static final Set<String> NON_DETERMINISTIC_FUNCTIONS = Set.of(
            "rand", "random", "uuid", "shuffle", "now", "secure_rand", "secure_random");

    private CteMaterializer() {}

    public record CteCandidate(String name, String bodySql, int referenceCount) {}

    public static List<CteCandidate> findCandidates(Statement statement)
    {
        if (!(statement instanceof Query query) || query.getWith().isEmpty()) {
            return ImmutableList.of();
        }
        With with = query.getWith().get();
        if (with.isRecursive()) {
            return ImmutableList.of();
        }
        List<WithQuery> withQueries = with.getQueries();
        Set<String> cteNames = withQueries.stream()
                .map(wq -> wq.getName().getValue().toLowerCase(ENGLISH))
                .collect(Collectors.toSet());

        List<CteCandidate> candidates = new ArrayList<>();
        for (WithQuery withQuery : withQueries) {
            String name = withQuery.getName().getValue().toLowerCase(ENGLISH);
            if (withQuery.getColumnNames().isPresent()) {
                continue;
            }
            if (referencesAnyCte(withQuery.getQuery(), cteNames)) {
                continue;
            }
            if (!isDeterministic(withQuery.getQuery())) {
                continue;
            }
            int referenceCount = countTableReferences(query, name);
            if (referenceCount < 2) {
                continue;
            }
            candidates.add(new CteCandidate(name, SqlFormatter.formatSql(withQuery.getQuery()), referenceCount));
        }
        return ImmutableList.copyOf(candidates);
    }

    /**
     * Rewrite the statement, replacing each materialized CTE's body with {@code SELECT * FROM <scratch>}.
     * {@code nameToScratch} maps lowercased CTE name to its fully-qualified scratch table name.
     */
    public static Statement rewrite(Statement statement, Map<String, String> nameToScratch, SqlParser parser)
    {
        Query query = (Query) statement;
        With with = query.getWith().orElseThrow();
        List<WithQuery> rewritten = with.getQueries().stream()
                .map(withQuery -> {
                    String name = withQuery.getName().getValue().toLowerCase(ENGLISH);
                    String scratch = nameToScratch.get(name);
                    if (scratch == null) {
                        return withQuery;
                    }
                    Query body = (Query) parser.createStatement("SELECT * FROM " + scratch);
                    return new WithQuery(withQuery.getName(), body, withQuery.getColumnNames());
                })
                .collect(Collectors.toList());
        With newWith = new With(with.isRecursive(), rewritten);
        return new Query(
                query.getSessionProperties(),
                query.getFunctions(),
                java.util.Optional.of(newWith),
                query.getQueryBody(),
                query.getOrderBy(),
                query.getOffset(),
                query.getLimit());
    }

    private static int countTableReferences(Node root, String cteName)
    {
        int[] count = {0};
        walk(root, node -> {
            if (node instanceof Table table && isUnqualified(table) && table.getName().getSuffix().equalsIgnoreCase(cteName)) {
                count[0]++;
            }
        });
        return count[0];
    }

    private static boolean referencesAnyCte(Node root, Set<String> cteNames)
    {
        boolean[] found = {false};
        walk(root, node -> {
            if (node instanceof Table table && isUnqualified(table) && cteNames.contains(table.getName().getSuffix().toLowerCase(ENGLISH))) {
                found[0] = true;
            }
        });
        return found[0];
    }

    private static boolean isDeterministic(Node root)
    {
        boolean[] deterministic = {true};
        walk(root, node -> {
            if (node instanceof FunctionCall functionCall
                    && NON_DETERMINISTIC_FUNCTIONS.contains(functionCall.getName().getSuffix().toLowerCase(ENGLISH))) {
                deterministic[0] = false;
            }
            // CURRENT_TIME / CURRENT_TIMESTAMP / CURRENT_DATE / CURRENT_USER / CURRENT_CATALOG / ...
            if (node.getClass().getSimpleName().startsWith("Current")) {
                deterministic[0] = false;
            }
        });
        return deterministic[0];
    }

    private static boolean isUnqualified(Table table)
    {
        return table.getName().getParts().size() == 1;
    }

    private interface NodeConsumer
    {
        void accept(Node node);
    }

    private static void walk(Node node, NodeConsumer consumer)
    {
        consumer.accept(node);
        for (Node child : node.getChildren()) {
            walk(child, consumer);
        }
    }
}
