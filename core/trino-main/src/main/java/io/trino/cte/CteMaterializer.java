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
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Locale.ENGLISH;

/**
 * Pure-AST detection and rewrite for staged CTE materialization.
 * <p>
 * Detection ({@link #findCandidates}) finds top-level WITH entries that are worth (and safe to)
 * materialize into a per-query scratch table. Rewrite ({@link #rewrite}) swaps each materialized CTE's
 * body for {@code SELECT * FROM <scratch>} in the main query; re-analysis re-resolves every reference to
 * the scratch scan, and predicate/projection pushdown still specializes per consumer.
 * <p>
 * A materialized CTE may itself reference earlier CTEs. {@link #buildScratchSource} constructs each
 * scratch CTAS so those inner references resolve: dependencies that were themselves materialized are
 * redefined as {@code SELECT * FROM <their scratch>}; dependencies that were not materialized are inlined
 * (their original bodies, transitively) via a WITH clause on the CTAS. Materializing in WITH-declaration
 * order guarantees a dependency's scratch table is committed before any CTE that reads it.
 * <p>
 * Eligibility gates (conservative — anything not matching falls back to normal inlining):
 * <ul>
 *   <li>statement is a {@link Query} with a non-recursive {@link With} and no CTE-dependency cycle;</li>
 *   <li>the CTE is referenced at least twice across the whole statement (main query + sibling bodies);</li>
 *   <li>the CTE has no explicit column-alias list (kept simple for now);</li>
 *   <li>the CTE and every CTE in its transitive dependency closure are deterministic (no
 *       rand/uuid/now/shuffle and no CURRENT_* contextual values) — otherwise materializing one
 *       evaluation and reusing it would change results;</li>
 *   <li>a CTE that depends on a sibling must not itself contain a nested WITH (so its body can be wrapped
 *       in a dependency WITH clause without merging scopes).</li>
 * </ul>
 */
public final class CteMaterializer
{
    // Nondeterministic / non-repeatable function names (lowercased, last name part).
    private static final Set<String> NON_DETERMINISTIC_FUNCTIONS = Set.of(
            "rand", "random", "uuid", "shuffle", "now", "secure_rand", "secure_random");

    private CteMaterializer() {}

    public record CteCandidate(String name, int referenceCount) {}

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
                .map(CteMaterializer::cteName)
                .collect(Collectors.toSet());

        // direct sibling-CTE dependencies of each CTE body
        Map<String, Set<String>> directDeps = new HashMap<>();
        for (WithQuery withQuery : withQueries) {
            directDeps.put(cteName(withQuery), referencedCtes(withQuery.getQuery(), cteNames));
        }
        // defensive: a valid non-recursive WITH cannot have a dependency cycle, but never risk infinite recursion
        if (hasCycle(cteNames, directDeps)) {
            return ImmutableList.of();
        }
        Map<String, WithQuery> byName = withQueries.stream()
                .collect(Collectors.toMap(CteMaterializer::cteName, wq -> wq, (a, b) -> a, LinkedHashMap::new));
        Map<String, Boolean> deterministic = new HashMap<>();

        List<CteCandidate> candidates = new ArrayList<>();
        for (WithQuery withQuery : withQueries) {
            String name = cteName(withQuery);
            if (withQuery.getColumnNames().isPresent()) {
                continue;
            }
            if (!isDeterministicTransitively(name, byName, directDeps, deterministic)) {
                continue;
            }
            // a CTE that wraps its body in a dependency WITH clause must not already carry its own WITH
            if (!directDeps.get(name).isEmpty() && bodyHasWith(withQuery.getQuery())) {
                continue;
            }
            int referenceCount = countTableReferences(query, name);
            if (referenceCount < 2) {
                continue;
            }
            candidates.add(new CteCandidate(name, referenceCount));
        }
        return ImmutableList.copyOf(candidates);
    }

    /**
     * Build the {@code AS}-source query for materializing CTE {@code cteName} into a scratch table.
     * Dependencies already present in {@code nameToScratch} are redefined as scratch scans; the rest are
     * inlined (their original bodies) via a WITH clause covering the CTE's transitive dependency closure.
     * Returns just the CTE body when it has no sibling dependencies.
     */
    public static String buildScratchSource(Statement statement, String cteName, Map<String, String> nameToScratch, SqlParser parser)
    {
        Query query = (Query) statement;
        List<WithQuery> withQueries = query.getWith().orElseThrow().getQueries();
        Set<String> cteNames = withQueries.stream().map(CteMaterializer::cteName).collect(Collectors.toSet());
        Map<String, WithQuery> byName = withQueries.stream()
                .collect(Collectors.toMap(CteMaterializer::cteName, wq -> wq, (a, b) -> a, LinkedHashMap::new));
        Map<String, Set<String>> directDeps = new HashMap<>();
        for (WithQuery withQuery : withQueries) {
            directDeps.put(cteName(withQuery), referencedCtes(withQuery.getQuery(), cteNames));
        }

        WithQuery target = byName.get(cteName.toLowerCase(ENGLISH));
        Set<String> closure = new LinkedHashSet<>();
        collectClosure(cteName.toLowerCase(ENGLISH), directDeps, closure);

        // WITH entries for the dependency closure, in original declaration order
        List<WithQuery> entries = new ArrayList<>();
        for (WithQuery withQuery : withQueries) {
            String name = cteName(withQuery);
            if (!closure.contains(name)) {
                continue;
            }
            String scratch = nameToScratch.get(name);
            if (scratch != null) {
                Query scan = (Query) parser.createStatement("SELECT * FROM " + scratch);
                entries.add(new WithQuery(withQuery.getName(), scan, Optional.empty()));
            }
            else {
                entries.add(withQuery);
            }
        }

        Query body = target.getQuery();
        Query source = entries.isEmpty()
                ? body
                : new Query(
                        body.getSessionProperties(),
                        body.getFunctions(),
                        Optional.of(new With(false, entries)),
                        body.getQueryBody(),
                        body.getOrderBy(),
                        body.getOffset(),
                        body.getLimit());
        return SqlFormatter.formatSql(source);
    }

    /**
     * Rewrite the statement, replacing each materialized CTE's body with {@code SELECT * FROM <scratch>}.
     * {@code nameToScratch} maps lowercased CTE name to its fully-qualified scratch table name. CTEs absent
     * from the map (not materialized) are left untouched.
     */
    public static Statement rewrite(Statement statement, Map<String, String> nameToScratch, SqlParser parser)
    {
        Query query = (Query) statement;
        With with = query.getWith().orElseThrow();
        List<WithQuery> rewritten = with.getQueries().stream()
                .map(withQuery -> {
                    String name = cteName(withQuery);
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
                Optional.of(newWith),
                query.getQueryBody(),
                query.getOrderBy(),
                query.getOffset(),
                query.getLimit());
    }

    /**
     * Base tables that a CTE body scans, for a stats-based cost estimate. Returns empty when the CTE
     * references a sibling CTE — its scanned volume then depends on inlined dependencies and is treated as
     * unknown by the caller (so the cost gate does not prune it). Otherwise returns every table name the
     * body reads (which, with no sibling dependencies, are all real catalog tables).
     */
    public static Optional<List<QualifiedName>> sourceTablesForCostEstimate(Statement statement, String cteName)
    {
        if (!(statement instanceof Query query) || query.getWith().isEmpty()) {
            return Optional.empty();
        }
        List<WithQuery> withQueries = query.getWith().get().getQueries();
        Set<String> cteNames = withQueries.stream().map(CteMaterializer::cteName).collect(Collectors.toSet());
        String target = cteName.toLowerCase(ENGLISH);
        WithQuery withQuery = withQueries.stream().filter(wq -> cteName(wq).equals(target)).findFirst().orElse(null);
        if (withQuery == null) {
            return Optional.empty();
        }
        if (!referencedCtes(withQuery.getQuery(), cteNames).isEmpty()) {
            return Optional.empty();
        }
        List<QualifiedName> tables = new ArrayList<>();
        walk(withQuery.getQuery(), node -> {
            if (node instanceof Table table) {
                tables.add(table.getName());
            }
        });
        return Optional.of(tables);
    }

    /**
     * First fully-qualified (catalog.schema.table) table reference in the statement, used to place scratch
     * tables when the session has no default catalog/schema. Empty if the statement references no such table.
     */
    public static Optional<QualifiedName> firstQualifiedTable(Statement statement)
    {
        QualifiedName[] found = {null};
        walk(statement, node -> {
            if (found[0] == null && node instanceof Table table && table.getName().getParts().size() == 3) {
                found[0] = table.getName();
            }
        });
        return Optional.ofNullable(found[0]);
    }

    private static String cteName(WithQuery withQuery)
    {
        return withQuery.getName().getValue().toLowerCase(ENGLISH);
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

    private static Set<String> referencedCtes(Node root, Set<String> cteNames)
    {
        Set<String> found = new LinkedHashSet<>();
        walk(root, node -> {
            if (node instanceof Table table && isUnqualified(table)) {
                String name = table.getName().getSuffix().toLowerCase(ENGLISH);
                if (cteNames.contains(name)) {
                    found.add(name);
                }
            }
        });
        return found;
    }

    private static void collectClosure(String name, Map<String, Set<String>> directDeps, Set<String> out)
    {
        for (String dep : directDeps.getOrDefault(name, Set.of())) {
            if (out.add(dep)) {
                collectClosure(dep, directDeps, out);
            }
        }
    }

    private static boolean isDeterministicTransitively(
            String name,
            Map<String, WithQuery> byName,
            Map<String, Set<String>> directDeps,
            Map<String, Boolean> memo)
    {
        Boolean cached = memo.get(name);
        if (cached != null) {
            return cached;
        }
        // guard against revisiting during recursion (cycles are already excluded, but stay safe)
        memo.put(name, true);
        boolean result = isDeterministicBody(byName.get(name).getQuery());
        if (result) {
            for (String dep : directDeps.getOrDefault(name, Set.of())) {
                if (!isDeterministicTransitively(dep, byName, directDeps, memo)) {
                    result = false;
                    break;
                }
            }
        }
        memo.put(name, result);
        return result;
    }

    private static boolean isDeterministicBody(Node root)
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

    private static boolean bodyHasWith(Query body)
    {
        return body.getWith().isPresent();
    }

    private static boolean hasCycle(Set<String> nodes, Map<String, Set<String>> edges)
    {
        Map<String, Integer> color = new HashMap<>(); // 0=visiting, 1=done
        for (String node : nodes) {
            if (!color.containsKey(node) && dfsHasCycle(node, edges, color)) {
                return true;
            }
        }
        return false;
    }

    private static boolean dfsHasCycle(String node, Map<String, Set<String>> edges, Map<String, Integer> color)
    {
        color.put(node, 0);
        for (String next : edges.getOrDefault(node, Set.of())) {
            Integer c = color.get(next);
            if (c == null) {
                if (dfsHasCycle(next, edges, color)) {
                    return true;
                }
            }
            else if (c == 0) {
                return true;
            }
        }
        color.put(node, 1);
        return false;
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
