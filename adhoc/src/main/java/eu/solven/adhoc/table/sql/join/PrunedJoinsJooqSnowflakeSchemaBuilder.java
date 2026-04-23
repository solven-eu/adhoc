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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jooq.Condition;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.google.common.collect.ImmutableSet;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Opt-in variant of {@link JooqSnowflakeSchemaBuilder} that records each {@code leftJoin} declaration lazily — without
 * eagerly folding them into {@code snowflakeTable}. Paired with {@link PrunedJoinsJooqTableSupplier}, which implements
 * the per-query pruning algorithm.
 * <p>
 * Split of responsibilities:
 * <ul>
 * <li><b>This class</b> — records the join tree as {@link JoinNode}s and exposes two materialisation entry points:
 * {@link #getSnowflakeTable()} (all joins — for schema introspection) and {@link #materialise(Set)
 * materialise(neededAliases)} (subset — called by the supplier).</li>
 * <li>{@link PrunedJoinsJooqTableSupplier} — owns the column→alias index, the needed-alias cache, the
 * {@link eu.solven.adhoc.table.sql.IJooqColumnsResolver columnsResolver}, and the {@code tableFor(TableQueryV4)}
 * pruning algorithm. Instantiated via {@code PrunedJoinsJooqTableSupplier.builder().schema(this).build()}.</li>
 * </ul>
 * Motivation: engines such as DuckDB pay a non-zero cost per join in the {@code FROM} clause, even when the join
 * contributes no columns to the {@code SELECT}. When the snowflake has many arms and most queries touch only a subset,
 * eagerly composing the full {@code baseTable.leftJoin(...).leftJoin(...)} chain wastes that budget.
 * <p>
 * Usage:
 *
 * <pre>
 * PrunedJoinsJooqSnowflakeSchemaBuilder schema = PrunedJoinsJooqSnowflakeSchemaBuilder.prunedBuilder()
 * 		.baseTable(DSL.table("fact"))
 * 		.baseTableAlias("fact")
 * 		.build();
 * schema.leftJoin("fact", DSL.table("dim_a"), "a", List.of(Map.entry("a_id", "id")), Set.of("a_name"))
 * 		.leftJoin("a", DSL.table("dim_a_sub"), "asub", List.of(Map.entry("sub_id", "id")), Set.of("sub_label"));
 *
 * IJooqTableSupplier supplier = PrunedJoinsJooqTableSupplier.builder().schema(schema).build();
 *
 * JooqTableWrapperParameters params = JooqTableWrapperParameters.builder()
 * 		.dslSupplier(dsl)
 * 		.table(schema.getSnowflakeTable()) // full table — used for schema introspection
 * 		.tableSupplier(supplier) // per-query pruned table
 * 		.build();
 * </pre>
 *
 * Only {@code LEFT JOIN}s registered via {@link #leftJoin} are prunable. Any join registered directly via
 * {@link #leftJoinConditions} is treated as {@code prunable=false} — it is always included — because its column
 * contract is unknown to the supplier.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class PrunedJoinsJooqSnowflakeSchemaBuilder extends JooqSnowflakeSchemaBuilder {

	/**
	 * Declaration order matters: when materialising, joins are folded onto the base in the order they were declared.
	 */
	@Getter
	private final List<JoinNode> joinNodes = new ArrayList<>();

	/** Memoised full-joins table (all joins included). Invalidated whenever a new {@code leftJoin} is declared. */
	private Table<Record> fullTableCache;

	/**
	 * A declaration of a LEFT JOIN to be composed into the {@code FROM} clause on demand.
	 */
	@Value
	@Builder(toBuilder = true)
	public static class JoinNode {
		/** The alias by which this join is referenced (both by its children in the snowflake chain and by queries). */
		String alias;

		/**
		 * The alias of the parent table this join is attached to. May be the base-table alias (direct join), or another
		 * join's alias for a snowflake chain.
		 */
		String parentAlias;

		/** The un-aliased {@code joinedTable} as declared by the caller. */
		Table<?> joinedTable;

		/** ON-clause conditions, already parsed by {@link JooqSnowflakeSchemaBuilder#parseOnName}. */
		List<Condition> onConditions;

		/** {@code true} if this join may be pruned when no column it provides is referenced. */
		boolean prunable;

		/**
		 * Explicit list of columns this join provides. When {@code null}, {@link PrunedJoinsJooqTableSupplier} asks its
		 * {@code columnsResolver} to derive the column set from the {@code joinedTable}.
		 */
		@Default
		Set<String> columnsOverride = null;
	}

	@Builder(builderMethodName = "prunedBuilder", builderClassName = "PrunedJoinsJooqSnowflakeSchemaBuilderBuilder")
	public PrunedJoinsJooqSnowflakeSchemaBuilder(Table<Record> baseTable, String baseTableAlias) {
		super(baseTable, baseTableAlias);
	}

	/**
	 * Exposes the base-table alias to {@link PrunedJoinsJooqTableSupplier} (package-private access would do, but a
	 * getter is clearer for future consumers outside this package).
	 */
	public String getBaseTableAlias() {
		return baseTableAlias;
	}

	// ── Recording joins (override the high-level path only) ─────────────────

	@SuppressWarnings("CPD-START")
	@Override
	public PrunedJoinsJooqSnowflakeSchemaBuilder leftJoin(String leftTableAlias,
			Table<?> joinedTable,
			String joinName,
			List<Map.Entry<String, String>> on) {
		// Same side-effects as the parent (aliaser registration + latestJoin tracking), but we do NOT accumulate
		// snowflakeTable — the FROM clause is rebuilt per-query by the supplier via `materialise(...)`.
		List<Condition> onConditions = on.stream().map(e -> {
			Name leftName = parseOnName(leftTableAlias, e.getKey());
			Name rightName = parseOnName(joinName, e.getValue());
			registerInAliaser(leftName, rightName);
			return DSL.field(leftName).eq(DSL.field(rightName));
		}).toList();

		this.latestJoin = joinName;

		joinNodes.add(JoinNode.builder()
				.alias(joinName)
				.parentAlias(leftTableAlias)
				.joinedTable(joinedTable)
				.onConditions(onConditions)
				.prunable(true)
				.build());

		invalidateCaches();
		return this;
	}

	/**
	 * Variant of {@link #leftJoin(String, Table, String, List)} that carries an explicit list of columns the joined
	 * table provides. Use this when the configured {@code columnsResolver} cannot discover the joined table's fields
	 * (e.g. a {@code DSL.table(name)} with no declared fields), or when you want to override the derived set.
	 */
	public PrunedJoinsJooqSnowflakeSchemaBuilder leftJoin(String leftTableAlias,
			Table<?> joinedTable,
			String joinName,
			List<Map.Entry<String, String>> on,
			Set<String> providedColumns) {
		leftJoin(leftTableAlias, joinedTable, joinName, on);
		// Patch the just-appended node with the explicit columns override.
		int lastIdx = joinNodes.size() - 1;
		JoinNode last = joinNodes.get(lastIdx);
		joinNodes.set(lastIdx, last.toBuilder().columnsOverride(ImmutableSet.copyOf(providedColumns)).build());
		invalidateCaches();
		return this;
	}

	@Override
	public JooqSnowflakeSchemaBuilder leftJoinConditions(Table<?> joinedTable, List<Condition> on) {
		// Direct low-level registration: the supplier lacks the alias/column semantics, so treat this join as
		// non-prunable (always included) and attach it under the most recent prunable parent (or the base table).
		String alias = joinedTable.getName();
		joinNodes.add(JoinNode.builder()
				.alias(alias)
				.parentAlias(latestJoin)
				.joinedTable(joinedTable)
				.onConditions(on)
				.prunable(false)
				.build());
		invalidateCaches();
		return this;
	}

	/**
	 * Drops the memoised full-joins table. Called on each {@link #leftJoin} so the next {@link #getSnowflakeTable()}
	 * includes the newly-registered node.
	 */
	@SuppressWarnings("PMD.NullAssignment")
	protected void invalidateCaches() {
		fullTableCache = null;
	}

	// ── Materialisation ─────────────────────────────────────────────────────

	/**
	 * Returns the full-joins table (every registered join folded in declaration order). This is what callers should
	 * pass to {@code JooqTableWrapperParameters.builder().table(...)} so schema introspection sees every column.
	 */
	@Override
	public Table<Record> getSnowflakeTable() {
		if (fullTableCache == null) {
			fullTableCache = materialise(allAliases());
		}
		return fullTableCache;
	}

	/**
	 * Builds a fresh {@code Table<Record>} by folding the registered joins (in declaration order) onto the base table,
	 * skipping any join whose alias is not in {@code neededAliases}. Called by
	 * {@link PrunedJoinsJooqTableSupplier#tableFor(eu.solven.adhoc.query.table.TableQueryV4)} for the per-query pruned
	 * table and by {@link #getSnowflakeTable()} for the all-joins table.
	 */
	public Table<Record> materialise(Set<String> neededAliases) {
		Table<Record> composed = baseTable.as(baseTableAlias);
		for (JoinNode node : joinNodes) {
			if (neededAliases.contains(node.getAlias())) {
				composed = composed.leftJoin(node.getJoinedTable().as(node.getAlias()))
						.on(node.getOnConditions().toArray(Condition[]::new));
			}
		}
		return composed;
	}

	private Set<String> allAliases() {
		Set<String> all = new LinkedHashSet<>();
		for (JoinNode node : joinNodes) {
			all.add(node.getAlias());
		}
		return all;
	}
}
