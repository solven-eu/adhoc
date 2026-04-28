/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jooq.Condition;
import org.jooq.Name;
import org.jooq.Parser;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.google.common.collect.Lists;

import eu.solven.adhoc.table.sql.AdhocJooqHelper;
import eu.solven.adhoc.table.sql.IDSLSupplier;
import eu.solven.adhoc.table.sql.IJooqTableSupplier;
import eu.solven.adhoc.table.sql.join.PrunedJoinsJooqTableSupplierBuilder.PrunedJoinsJooqTableSupplierBuilderBuilder;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * This class helps building a star/snowflake schema, around some baseStore and various LEFT JOINs to provide additional
 * columns.
 * <p>
 * This also facilitate the registration of proper transcoder. This implementation give priority to the first encounter
 * of a field, giving priority to the LEFT table over the RIGHT table.
 * 
 * @author Benoit Lacelle
 */
public class JooqTableSupplierBuilder {
	// JOINs often refers the same name from the joined tables: this map will record the qualified field to refer when
	// the unqualified field is queried
	// e.g. if `FROM table t JOIN joined j ON t.k = j.k`, then may decide that `k` always refers to `t.k`
	@NonNull
	// @Builder.Default
	// BEWARE Naming should follow MapTableAliaser
	@Getter
	final Map<String, String> aliasToOriginal = new ConcurrentHashMap<>();

	@NonNull
	final IDSLSupplier dslSupplier;

	@NonNull
	@Getter
	final Table<Record> baseTable;
	@NonNull
	final String baseTableAlias;

	@Getter
	Table<Record> snowflakeTable;

	String latestJoin;

	// @NonNull
	// @Default
	Supplier<Parser> parserSupplier = DSL.using(SQLDialect.DUCKDB)::parser;

	// https://stackoverflow.com/questions/30717640/how-to-exclude-property-from-lombok-builder
	// `snowflakeTable` is not built by the builder
	@Builder(builderMethodName = "legacyBuilder")
	public JooqTableSupplierBuilder(IDSLSupplier dslSupplier, Table<Record> baseTable, String baseTableAlias) {
		this.dslSupplier = dslSupplier;
		this.baseTable = baseTable;
		this.baseTableAlias = baseTableAlias;

		this.snowflakeTable = baseTable.as(baseTableAlias);

		this.latestJoin = baseTableAlias;
	}

	public static PrunedJoinsJooqTableSupplierBuilderBuilder builder() {
		return PrunedJoinsJooqTableSupplierBuilder.prunedBuilder();
	}

	/**
	 * Add a LEFT JOIN configured through a chainable {@link JooqJoinBuilder} (the new primary API).
	 *
	 * <p>
	 * The {@code consumer} populates a fresh {@link JooqJoinBuilder} (table, alias, ON columns, optional per-join
	 * column aliases). Once it returns, the JOIN is committed against the schema. Compared to the legacy
	 * {@code leftJoin(leftAlias, joinedTable, joinName, on)} signature, this form is easier to read with multiple JOINs
	 * in a row and easier to skip dynamically (just wrap the call in an {@code if (envFlag) ...} block).
	 *
	 * <p>
	 * Example:
	 *
	 * <pre>{@code
	 * schema.leftJoin(j -> j.table(DSL.table("orders"))
	 * 		.alias("orders")
	 * 		.from("lineitem") // optional
	 * 		.on("l_orderkey", "o_orderkey")
	 * 		.withAlias("display", "name"));
	 * }</pre>
	 *
	 * @param consumer
	 *            populates the per-join builder.
	 * @return this
	 */
	public JooqTableSupplierBuilder leftJoin(Consumer<JooqJoinBuilder> consumer) {
		JooqJoinBuilder joinBuilder = new JooqJoinBuilder();
		consumer.accept(joinBuilder);
		// Empty-consumer fast path: the caller chose to skip this JOIN at runtime (e.g. wrapped in a feature
		// flag that turned out false). Drop it cleanly — no validation, no SQL change.
		if (joinBuilder.isEmpty()) {
			return this;
		}
		joinBuilder.validate();
		// Default `from` to the BASE table — the dominant pattern in OLAP/BI is a star schema where every
		// dimension JOINs the fact directly. Snowflake legs explicitly opt-in by calling `.from(prevJoin)`.
		// Argument: defaulting to base gives a louder failure on a missing `.from(...)` (the JOIN references
		// base columns the parent doesn't have → SQL error) versus the silent-misjoin you'd get if we defaulted
		// to the previous JOIN and the dim happened to share a column name with it.
		String fromAlias = Optional.ofNullable(joinBuilder.getFrom()).orElse(baseTableAlias);
		leftJoin(fromAlias, joinBuilder.getTable(), joinBuilder.getAlias(), joinBuilder.getOn());

		// Commit per-join column aliases via the existing schema-level facility — the join's alias is now the
		// `latestJoin`, so withAliases qualifies columns against the freshly-joined table.
		if (!joinBuilder.getColumnAliases().isEmpty()) {
			withAliases(joinBuilder.getColumnAliases());
		}
		return this;
	}

	/**
	 * Assumes the LEFT table is the base store.
	 *
	 * @param joinedTable
	 *            the JOINed table
	 * @param joinName
	 *            the alias of the JOINed table
	 * @param on
	 *            a {@link List} of mapping of `.isEqualTo` between LEFT fields and RIGHT fields
	 * @return
	 * @deprecated Prefer {@link #leftJoin(Consumer)} — chainable, easier to read with multiple JOINs, and easier to
	 *             skip dynamically.
	 */
	@Deprecated(since = "Prefer leftJoin(Consumer)")
	public JooqTableSupplierBuilder leftJoin(Table<?> joinedTable,
			String joinName,
			List<Map.Entry<String, String>> on) {
		return leftJoin(baseTableAlias, joinedTable, joinName, on);
	}

	// TODO Should this rely on `AdhocJooqHelper.name(name, dslContext::parser);`?
	// Coding it manually looks bad. But this syntax may not depends on the parser (e.g. DuckDB vs another syntax)
	protected Name parseOnName(String leftTableAlias, String rawName) {
		if (rawName.startsWith(";")) {
			return DSL.unquotedName(rawName.substring(1));
		} else {
			Name name = AdhocJooqHelper.name(rawName, parserSupplier);

			if (name.parts().length == 1) {
				name = DSL.name(DSL.name(leftTableAlias), name);
			}

			return name;
		}
	}

	/**
	 * @param leftTableAlias
	 *            the name of the LEFT/base table. May not be the root/base table given we manage snowflake schema.
	 * @param joinedTable
	 *            the JOINed table
	 * @param joinName
	 *            the alias of the JOINed table
	 * @param on
	 *            a {@link List} of mapping of `.isEqualTo` between LEFT fields and RIGHT fields
	 * @return
	 * @deprecated Prefer {@link #leftJoin(Consumer)} — chainable, easier to read with multiple JOINs, and easier to
	 *             skip dynamically. Used internally to commit a {@link JooqJoinBuilder}.
	 */
	@Deprecated(since = "Prefer leftJoin(Consumer)")
	public JooqTableSupplierBuilder leftJoin(String leftTableAlias,
			Table<?> joinedTable,
			String joinName,
			List<Map.Entry<String, String>> on) {
		List<Condition> onConditions = on.stream().map(e -> {
			Name leftName = parseOnName(leftTableAlias, e.getKey());
			Name rightName = parseOnName(joinName, e.getValue());

			registerInAliaser(leftName, rightName);

			return DSL.field(leftName).eq(DSL.field(rightName));
		}).toList();

		this.latestJoin = joinName;

		return leftJoinConditions(joinedTable.as(joinName), onConditions);
	}

	/**
	 * @deprecated Prefer {@link #leftJoin(Consumer)} with {@link JooqJoinBuilder#onSame(String)}.
	 */
	@Deprecated(since = "Prefer leftJoin(Consumer) + onSame(...)")
	public JooqTableSupplierBuilder joinHomo(String leftTableAlias,
			Table<?> joinedTable,
			String joinName,
			List<String> on) {
		return leftJoin(leftTableAlias, joinedTable, joinName, on.stream().map(f -> Map.entry(f, f)).toList());
	}

	/**
	 * @deprecated Prefer {@link #leftJoin(Consumer)} with {@link JooqJoinBuilder#onSame(String)}.
	 */
	@Deprecated(since = "Prefer leftJoin(Consumer) + onSame(...)")
	public JooqTableSupplierBuilder joinHomo(String leftTableAlias,
			Table<?> joinedTable,
			String joinName,
			String on,
			String... moreOn) {
		return joinHomo(leftTableAlias, joinedTable, joinName, Lists.asList(on, moreOn));
	}

	protected void registerInAliaser(Name leftName, Name rightName) {
		// `putIfAbsent`: priority to the first occurrence of the field
		// `leftTableAlias`; priority to the LEFT/base table than the RIGHT/joined table
		String queryName = leftName.last();
		String tableName = leftName.toString();

		if (!Objects.equals(queryName, tableName)) {
			aliasToOriginal.putIfAbsent(queryName, tableName);
		}
	}

	public JooqTableSupplierBuilder leftJoinConditions(Table<?> joinedTable, List<Condition> on) {
		snowflakeTable = snowflakeTable.leftJoin(joinedTable).on(on.toArray(Condition[]::new));

		// BEWARE Should we set `latestJoin` to null? No as it may has just been set by the caller. But if it is not the
		// case, we should reset it to null as current JOIN is not aliased.

		return this;
	}

	/**
	 *
	 * @param builderConsumer
	 *            enable chaining definitions without inlining
	 * @return this
	 */
	public JooqTableSupplierBuilder accept(Consumer<JooqTableSupplierBuilder> builderConsumer) {
		builderConsumer.accept(this);

		return this;
	}

	/**
	 *
	 * @param builderConsumers
	 *            enable chaining definitions without inlining
	 * @return this
	 */
	public JooqTableSupplierBuilder accept(Iterable<Consumer<JooqTableSupplierBuilder>> builderConsumers) {
		builderConsumers.forEach(builderConsumer -> builderConsumer.accept(this));

		return this;
	}

	/**
	 * 
	 * Register an alias mapping to the latest registered table.
	 * 
	 * @param alias
	 *            the name of the column as it should be referrable through ITableWrapper
	 * @param original
	 *            the name of the column in the table. Do not qualify it for current table
	 * @return current builder
	 */
	public JooqTableSupplierBuilder withAlias(String alias, String original) {
		return withAliases(Map.of(alias, original));
	}

	/**
	 * @return an {@link IJooqTableSupplier} suitable for
	 *         {@code JooqTableWrapperParameters.builder().tableSupplier(...)}. The base implementation returns a
	 *         constant supplier wrapping {@link #getSnowflakeTable()}; subclasses with per-query strategies (e.g.
	 *         {@link PrunedJoinsJooqTableSupplierBuilder}) override this to return a schema-aware supplier.
	 */
	public IJooqTableSupplier build() {
		return IJooqTableSupplier.constant(getSnowflakeTable());
	}

	public JooqTableSupplierBuilder withAliases(Map<String, String> aliasToOriginal) {
		if (latestJoin == null) {
			throw new IllegalStateException("Can not register an alias over an unamed JOIN");
		}

		aliasToOriginal.forEach(
				(alias, original) -> this.aliasToOriginal.put(alias, parseOnName(latestJoin, original).toString()));

		return this;
	}

}
