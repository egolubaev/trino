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

/**
 * Controls when a multiply-referenced, eligible CTE is materialized into a per-query scratch table
 * (see {@link CteMaterializer} / {@code SqlQueryExecution.maybeMaterializeCtes}).
 *
 * <ul>
 *   <li>{@link #NONE} — never materialize; CTEs are inlined per reference (stock Trino behaviour).</li>
 *   <li>{@link #ALL} — materialize every eligible CTE (referenced at least twice).</li>
 *   <li>{@link #HEURISTIC} — materialize an eligible CTE only when its reference count reaches
 *       {@code cte_materialization_min_references}. (A cost-model gate layers on top of this in a
 *       later milestone; for now the heuristic is the reference-count threshold.)</li>
 * </ul>
 */
public enum CteMaterializationStrategy
{
    NONE,
    ALL,
    HEURISTIC,
}
