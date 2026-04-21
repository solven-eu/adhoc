/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.table.sql.join;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jooq.Field;
import org.jooq.TableLike;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.sql.IJooqColumnsResolver;
import eu.solven.adhoc.table.sql.IJooqTableSupplier;
import eu.solven.adhoc.table.sql.JooqColumnsHelpers;
import eu.solven.adhoc.table.sql.join.PrunedJoinsJooqSnowflakeSchemaBuilder.JoinNode;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link IJooqTableSupplier} that prunes the {@code FROM} clause of a query to the minimum snowflake subset needed by
 * the referenced columns.
 * <p>
 * This is the pruning half of the two-responsibility split with {@link PrunedJoinsJooqSnowflakeSchemaBuilder}: the
 * builder records the join tree and materialises jOOQ {@code Table<Record>}s on demand, while this supplier runs the
 * per-query pruning algorithm —
 * <ol>
 * <li>collect referenced columns from the query (group-by + aggregator columns + filter columns);</li>
 * <li>look each column up in the column→alias index, populated from each {@link JoinNode}'s {@code columnsOverride} or
 * {@link #columnsResolver};</li>
 * <li>close over parent links (snowflake transitivity) and force-include any non-prunable node;</li>
 * <li>delegate back to {@link PrunedJoinsJooqSnowflakeSchemaBuilder#materialise(Set)} for the final
 * {@code Table<Record>}.</li>
 * </ol>
 * The supplier caches the pruning decision per referenced-column set (bounded LRU), and caches per-alias resolver
 * output. Both caches are cleared by {@link #purgeColumnCache()}.
 *
 * <p>
 * The supplier assumes the schema (the underlying {@link PrunedJoinsJooqSnowflakeSchemaBuilder}) is stable once queries
 * start flowing. If new {@code leftJoin}s are added afterwards, call {@link #purgeColumnCache()} to drop stale caches.
 *
 * @author Benoit Lacelle
 */
@Slf4j
@Builder
public class PrunedJoinsJooqTableSupplier implements IJooqTableSupplier {

	/** The snowflake-schema source: provides the {@link JoinNode} list and materialises {@code Table}s on demand. */
	@NonNull
	private final PrunedJoinsJooqSnowflakeSchemaBuilder schema;

	/**
	 * Strategy used to discover a join's columns when no {@code columnsOverride} was supplied on the {@link JoinNode}.
	 * Defaults to {@link JooqColumnsHelpers#fromJooqFields()}. Swap in
	 * {@link JooqColumnsHelpers#dbProbe(eu.solven.adhoc.table.sql.IDSLSupplier)} when the jOOQ tables carry no declared
	 * fields.
	 */
	@NonNull
	@Default
	private final IJooqColumnsResolver columnsResolver = JooqColumnsHelpers.fromJooqFields();

	/** Lazily built column→owning-join index. {@code null} means "not yet computed". Memoised across queries. */
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	private Map<String, String> columnToAlias;

	/**
	 * Per-alias cache of the resolved column set, to avoid calling {@link #columnsResolver} repeatedly (especially when
	 * the resolver does DB-side work). Cleared by {@link #purgeColumnCache()}.
	 */
	private final Map<String, Set<String>> resolvedColumnsByAlias = new LinkedHashMap<>();

	/**
	 * Bounded cache keyed by referenced-column set → needed-alias set. Per the design note, the {@code Table<Record>}
	 * rebuild cost is negligible, so we cache only the pruning decision, not the materialised table.
	 */
	private final Map<Set<String>, Set<String>> neededAliasCache = new LinkedHashMap<>(32, 0.75f, true) {
		private static final long serialVersionUID = 1L;
		private static final int MAX_ENTRIES = 128;

		@Override
		protected boolean removeEldestEntry(Map.Entry<Set<String>, Set<String>> eldest) {
			return size() > MAX_ENTRIES;
		}
	};

	// ── IJooqTableSupplier ──────────────────────────────────────────────────

	@Override
	public TableLike<?> tableFor(TableQueryV4 tableQuery) {
		Set<String> referenced = ImmutableSet.copyOf(collectReferencedColumns(tableQuery));
		Set<String> neededAliases =
				neededAliasCache.computeIfAbsent(referenced, r -> ImmutableSet.copyOf(computeNeededAliases(r)));
		return schema.materialise(neededAliases);
	}

	/**
	 * Drops the column→alias index, the per-query needed-alias cache, and the per-alias resolver cache. Call this after
	 * a late {@code leftJoin} on the underlying schema, after swapping {@link #columnsResolver}, or after the joined
	 * tables' columns change at runtime.
	 */
	@SuppressWarnings("PMD.NullAssignment")
	public void purgeColumnCache() {
		columnToAlias = null;
		neededAliasCache.clear();
		resolvedColumnsByAlias.clear();
	}

	// ── Pruning algorithm ───────────────────────────────────────────────────

	/**
	 * Starting from the referenced-column set, resolve each column to its owning join (via the column→alias index) and
	 * close over parent links so that any ancestor join required to reach a needed child is also marked. Non-prunable
	 * joins are forced in unconditionally (and their ancestors too).
	 */
	protected Set<String> computeNeededAliases(Set<String> referencedColumns) {
		Map<String, String> index = columnToAlias();
		String baseAlias = schema.getBaseTableAlias();
		List<JoinNode> joinNodes = schema.getJoinNodes();
		Set<String> needed = new LinkedHashSet<>();

		// 1. Direct hits from referenced columns.
		// Unknown columns (not in the index) are treated as base-table columns — matching the existing convention
		// of `aliasToOriginal`, where an unqualified name that isn't recorded defaults to the base. Callers who
		// want pruning to be correct for a column that isn't discoverable via `joinedTable.fields()` must declare
		// it via the explicit `providedColumns` override on `leftJoin(...)`.
		for (String col : referencedColumns) {
			String owner = index.get(col);
			if (owner != null && !owner.equals(baseAlias)) {
				needed.add(owner);
			}
		}

		// 2. Non-prunable joins are always needed.
		for (JoinNode node : joinNodes) {
			if (!node.isPrunable()) {
				needed.add(node.getAlias());
			}
		}

		// 3. Transitive closure up the parent chain.
		Map<String, JoinNode> byAlias = byAlias();
		Deque<String> worklist = new ArrayDeque<>(needed);
		while (!worklist.isEmpty()) {
			JoinNode node = byAlias.get(worklist.pop());
			if (node == null) {
				continue;
			}
			String parent = node.getParentAlias();
			if (parent != null && !parent.equals(baseAlias) && needed.add(parent)) {
				worklist.push(parent);
			}
		}

		return needed;
	}

	/**
	 * Collects every table-column reference in {@code tableQuery}: grouped-by columns, aggregator source columns,
	 * filter columns (global + per-aggregator {@code FILTER(WHERE ...)} clauses).
	 */
	protected Set<String> collectReferencedColumns(TableQueryV4 tableQuery) {
		return TableQueryV4.getColumns(tableQuery);
	}

	// ── Column→alias index ──────────────────────────────────────────────────

	/**
	 * Builds (or returns the memoised) {@code Map<columnName, joinAlias>}. The index is populated from each
	 * {@link JoinNode}'s {@code columnsOverride} if present, else from {@link #columnsResolver}. "Left wins" on
	 * collisions: the first-declared alias for a column keeps it (matches the existing {@code registerInAliaser}
	 * semantics in {@link JooqSnowflakeSchemaBuilder}).
	 */
	protected Map<String, String> columnToAlias() {
		if (columnToAlias == null) {
			Map<String, String> idx = new LinkedHashMap<>();
			for (JoinNode node : schema.getJoinNodes()) {
				Set<String> columns = resolveColumns(node);
				for (String column : columns) {
					// Unqualified form: first-declared wins on collisions (matches `registerInAliaser` /
					// `putIfAbsent` semantics).
					idx.putIfAbsent(column, node.getAlias());
					// Qualified form: `alias.column` always resolves unambiguously to this join's alias.
					idx.putIfAbsent(node.getAlias() + "." + column, node.getAlias());
				}
			}
			columnToAlias = idx;
		}
		return columnToAlias;
	}

	/**
	 * Resolves the column set for a {@link JoinNode}: {@code columnsOverride} if present, else delegates to
	 * {@link #columnsResolver} and caches the result per alias so the resolver is invoked at most once per alias per
	 * {@link #purgeColumnCache()} cycle (important when the resolver performs a DB round-trip).
	 */
	protected Set<String> resolveColumns(JoinNode node) {
		if (node.getColumnsOverride() != null) {
			return node.getColumnsOverride();
		}
		return resolvedColumnsByAlias.computeIfAbsent(node.getAlias(), alias -> {
			List<Field<?>> fields = columnsResolver.columnsOf(node.getJoinedTable());
			if (fields == null || fields.isEmpty()) {
				log.debug("Join-pruning: columnsResolver returned no fields for joinedTable={} (alias={}) —"
						+ " this join will not be prunable unless a columnsOverride is supplied on leftJoin(...)",
						node.getJoinedTable(),
						alias);
				return Set.of();
			}
			Set<String> probed = new LinkedHashSet<>();
			for (Field<?> f : fields) {
				probed.add(f.getName());
			}
			return probed;
		});
	}

	// ── Small helpers ───────────────────────────────────────────────────────

	private Map<String, JoinNode> byAlias() {
		Map<String, JoinNode> byAlias = new LinkedHashMap<>();
		for (JoinNode node : schema.getJoinNodes()) {
			byAlias.put(node.getAlias(), node);
		}
		return byAlias;
	}

	/**
	 * Returns a read-only snapshot of the current column→alias index — mainly for tests and debugging.
	 */
	@VisibleForTesting
	Map<String, String> getColumnToAliasSnapshot() {
		return ImmutableMap.copyOf(columnToAlias());
	}
}
