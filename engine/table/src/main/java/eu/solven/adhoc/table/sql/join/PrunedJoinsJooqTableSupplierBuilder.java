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
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.jooq.Condition;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jspecify.annotations.Nullable;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.table.sql.IDSLSupplier;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Opt-in variant of {@link JooqTableSupplierBuilder} that records each {@code leftJoin} declaration lazily — without
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
 * PrunedJoinsJooqTableSupplierBuilder schema = PrunedJoinsJooqTableSupplierBuilder.prunedBuilder()
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
public class PrunedJoinsJooqTableSupplierBuilder extends JooqTableSupplierBuilder {

	/**
	 * Declaration order matters: when materialising, joins are folded onto the base in the order they were declared.
	 */
	@Getter
	private final List<JoinNode> joinNodes = new ArrayList<>();

	/**
	 * Optional explicit list of columns the BASE table provides — escape hatch mirroring per-join
	 * {@code providedColumns}. Honoured by {@link PrunedJoinsJooqTableSupplier#resolveBaseColumns()}: when non-empty,
	 * the supplier uses this set verbatim and skips the resolver. Useful when the resolver cannot discover the base
	 * table's fields (typical with {@code DSL.table(name)} that carries no declared fields and no DB probe).
	 * <p>
	 * Empty default — falls back to the configured {@code columnsResolver}.
	 */
	@Getter
	@SuppressWarnings("PMD.AvoidFieldNameMatchingMethodName")
	private Set<String> baseProvidedColumns = Set.of();

	/**
	 * Declare the columns the base table provides. Subsequent {@code build()} / re-probes will see them. Calling this
	 * after queries have flowed requires {@link PrunedJoinsJooqTableSupplier#invalidateAll()} to drop the stale index.
	 */
	public PrunedJoinsJooqTableSupplierBuilder baseProvidedColumns(Set<String> columns) {
		this.baseProvidedColumns = ImmutableSet.copyOf(columns);
		return this;
	}

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

		/** ON-clause conditions, already parsed by {@link JooqTableSupplierBuilder#parseOnName}. */
		List<Condition> onConditions;

		/** {@code true} if this join may be pruned when no column it provides is referenced. */
		boolean prunable;

		/**
		 * Explicit list of columns this join provides. When {@code null}, {@link PrunedJoinsJooqTableSupplier} asks its
		 * {@code columnsResolver} to derive the column set from the {@code joinedTable}.
		 */
		@Default
		Set<String> columnsOverride = null;

		/**
		 * Aliases referenced by the ON-clause beyond {@link #parentAlias}. Captures the diamond-join case where the
		 * ON-clause names columns from multiple parent tables (e.g.
		 * {@code c.region = a.region AND c.segment = b.segment} — declared parent {@code a}, additional reference
		 * {@code b}). Populated at parse time from the qualified parts of each ON-clause Name; consumed by
		 * {@link PrunedJoinsJooqTableSupplier#computeNeededAliases} to widen the prune-time dependency closure so
		 * neither parent gets dropped.
		 */
		@Default
		Set<String> referencedAliases = ImmutableSet.of();
	}

	@Builder(builderMethodName = "prunedBuilder", builderClassName = "PrunedJoinsJooqTableSupplierBuilderBuilder")
	public PrunedJoinsJooqTableSupplierBuilder(IDSLSupplier dslSupplier,
			Table<Record> baseTable,
			String baseTableAlias) {
		super(dslSupplier, baseTable, baseTableAlias);
	}

	/**
	 * Exposes the base-table alias to {@link PrunedJoinsJooqTableSupplier} (package-private access would do, but a
	 * getter is clearer for future consumers outside this package).
	 */
	public String getBaseTableAlias() {
		return baseTableAlias;
	}

	// ── Recording joins (override the high-level path only) ─────────────────

	/**
	 * Honours {@link JooqJoinBuilder#providedColumns(Set)} — when the consumer sets it, the recorded {@link JoinNode}
	 * carries that explicit override (used for prunability decisions).
	 */
	@Override
	public PrunedJoinsJooqTableSupplierBuilder leftJoin(Consumer<JooqJoinBuilder> consumer) {
		JooqJoinBuilder joinBuilder = new JooqJoinBuilder();
		consumer.accept(joinBuilder);
		// Empty-consumer fast path — same rationale as JooqTableSupplierBuilder: the JOIN is silently
		// dropped, the joinNodes list is left untouched.
		if (joinBuilder.isEmpty()) {
			return this;
		}
		joinBuilder.validate();
		// See JooqTableSupplierBuilder#leftJoin(Consumer) for the rationale: default to the BASE table, not
		// the most-recent join. Star pattern is dominant; snowflake legs opt-in via `.from(prevJoin)`.
		String fromAlias = Optional.ofNullable(joinBuilder.getFrom()).orElse(baseTableAlias);
		Set<String> provided = joinBuilder.getProvidedColumns();
		Set<String> columnsOverride;
		if (provided == null) {
			columnsOverride = null;
		} else {
			columnsOverride = ImmutableSet.copyOf(provided);
		}

		recordJoin(fromAlias,
				joinBuilder.getTable(),
				joinBuilder.getAlias(),
				joinBuilder.getOn(),
				joinBuilder.isPrunable(),
				columnsOverride);

		if (!joinBuilder.getColumnAliases().isEmpty()) {
			withAliases(joinBuilder.getColumnAliases());
		}
		return this;
	}

	@SuppressWarnings("CPD-START")
	@Override
	public PrunedJoinsJooqTableSupplierBuilder leftJoin(String leftTableAlias,
			Table<?> joinedTable,
			String joinName,
			List<Map.Entry<String, String>> on) {
		recordJoin(leftTableAlias, joinedTable, joinName, on, true, null);
		return this;
	}

	/**
	 * Single point of {@link JoinNode} construction: parses the ON-clause, registers it in the aliaser, harvests
	 * referenced aliases, and appends a fully-formed node to {@link #joinNodes}. Replaces the previous
	 * "append-then-patch-the-last-element" idiom: every caller passes the values it actually wants, and the node is
	 * built right the first time.
	 *
	 * @param columnsOverride
	 *            explicit list of columns this join provides, or {@code null} to defer to the supplier's
	 *            {@code columnsResolver}.
	 */
	protected void recordJoin(String leftTableAlias,
			Table<?> joinedTable,
			String joinName,
			List<Map.Entry<String, String>> on,
			boolean prunable,
			@Nullable Set<String> columnsOverride) {
		// Same side-effects as the parent (aliaser registration + latestJoin tracking), but we do NOT accumulate
		// snowflakeTable — the FROM clause is rebuilt per-query by the supplier via `materialise(...)`.
		ImmutableSet.Builder<String> referencedAliases = ImmutableSet.builder();
		List<Condition> onConditions = on.stream().map(e -> {
			Name leftName = parseOnName(leftTableAlias, e.getKey());
			Name rightName = parseOnName(joinName, e.getValue());
			// Harvest the alias prefix of every fully-qualified Name. parseOnName auto-prefixes unqualified
			// columns with the declared left/joined alias, so any 2+-part Name carries the alias as parts()[0].
			collectAlias(leftName, referencedAliases);
			collectAlias(rightName, referencedAliases);
			registerInAliaser(leftName, rightName);
			return DSL.field(leftName).eq(DSL.field(rightName));
		}).toList();

		this.latestJoin = joinName;

		joinNodes.add(JoinNode.builder()
				.alias(joinName)
				.parentAlias(leftTableAlias)
				.joinedTable(joinedTable)
				.onConditions(onConditions)
				.referencedAliases(referencedAliases.build())
				.prunable(prunable)
				.columnsOverride(columnsOverride)
				.build());
	}

	/**
	 * Adds the alias prefix of {@code name} to {@code sink}, if the {@code Name} is qualified (2+ parts). Used to
	 * harvest the set of aliases an ON-clause depends on, so the prune-time closure can keep every referenced parent
	 * alive even when it isn't the one declared via {@code parentAlias}.
	 */
	private static void collectAlias(Name name, ImmutableSet.Builder<String> sink) {
		Name[] parts = name.parts();
		if (parts.length >= 2) {
			// TODO Beware `[0]` may be a database name instead of a table name
			sink.add(parts[0].last());
		}
	}

	/**
	 * Variant of {@link #leftJoin(String, Table, String, List)} that carries an explicit list of columns the joined
	 * table provides. Use this when the configured {@code columnsResolver} cannot discover the joined table's fields
	 * (e.g. a {@code DSL.table(name)} with no declared fields), or when you want to override the derived set.
	 *
	 * @deprecated Prefer {@link #leftJoin(Consumer)} with {@link JooqJoinBuilder#providedColumns(Set)}.
	 */
	@Deprecated(since = "Prefer leftJoin(Consumer) + providedColumns(...)")
	public PrunedJoinsJooqTableSupplierBuilder leftJoin(String leftTableAlias,
			Table<?> joinedTable,
			String joinName,
			List<Map.Entry<String, String>> on,
			Set<String> providedColumns) {
		recordJoin(leftTableAlias, joinedTable, joinName, on, true, ImmutableSet.copyOf(providedColumns));
		return this;
	}

	/**
	 * @return a {@link PrunedJoinsJooqTableSupplier} bound to this schema. Use as
	 *         {@code JooqTableWrapperParameters.builder().tableSupplier(schema.build())}.
	 */
	@Override
	public PrunedJoinsJooqTableSupplier build() {
		return PrunedJoinsJooqTableSupplier.builder().schema(this).build();
	}

	@Override
	public JooqTableSupplierBuilder leftJoinConditions(Table<?> joinedTable, List<Condition> on) {
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
		return this;
	}

	// ── Materialisation ─────────────────────────────────────────────────────

	/**
	 * Returns the full-joins table (every registered join folded in declaration order). This is what callers should
	 * pass to {@code JooqTableWrapperParameters.builder().table(...)} so schema introspection sees every column.
	 */
	@Override
	public Table<Record> getSnowflakeTable() {
		return materialise(allAliases());
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
