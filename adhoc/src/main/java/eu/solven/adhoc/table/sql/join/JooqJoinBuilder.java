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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jooq.Table;

import lombok.AccessLevel;
import lombok.Getter;

/**
 * Per-join builder produced inside {@link JooqSnowflakeSchemaBuilder#leftJoin(java.util.function.Consumer)}.
 *
 * <p>
 * Replaces the multi-parameter {@code leftJoin(leftAlias, joinedTable, joinName, on)} signature with a chainable DSL
 * that scopes per-join state (the LEFT side, the joined table, the alias, the ON columns, and per-column aliases) to
 * the consumer's lifetime. Once the consumer returns, {@link JooqSnowflakeSchemaBuilder} commits the join via the same
 * {@link JooqSnowflakeSchemaBuilder#leftJoinConditions} machinery the legacy overloads use.
 *
 * <p>
 * Example:
 *
 * <pre>{@code
 * schema.leftJoin(j -> j.table(DSL.table("orders"))
 * 		.alias("orders")
 * 		// `.from(...)` is optional — defaults to the BASE table (the schema's central fact). Set it on
 * 		// snowflake legs where the LEFT side is a previously-joined dimension.
 * 		.on("l_orderkey", "o_orderkey")
 * 		.onSame("region_id") // shortcut for homo-join columns
 * 		.withAlias("display", "name")); // per-join column aliases
 * }</pre>
 *
 * @author Benoit Lacelle
 */
public class JooqJoinBuilder {
	@Getter(AccessLevel.PACKAGE)
	private Table<?> table;
	@Getter(AccessLevel.PACKAGE)
	private String alias;
	@Getter(AccessLevel.PACKAGE)
	private String from;
	// LEFT-name → RIGHT-name pairs. Order preserved.
	@Getter(AccessLevel.PACKAGE)
	private final List<Map.Entry<String, String>> on = new ArrayList<>();
	// Per-join column aliases applied AFTER the JOIN is committed. LinkedHashMap preserves declaration order
	// so the resulting aliases are deterministic.
	@Getter(AccessLevel.PACKAGE)
	private final Map<String, String> columnAliases = new LinkedHashMap<>();
	// Optional explicit column list — only honoured by PrunedJoinsJooqSnowflakeSchemaBuilder, where it overrides
	// the columns the join provides for prunability decisions. Null on the base schema builder (ignored).
	@Getter(AccessLevel.PACKAGE)
	private Set<String> providedColumns;

	/**
	 * @param joinedTable
	 *            the JOOQ table being joined.
	 * @return this
	 */
	public JooqJoinBuilder table(Table<?> joinedTable) {
		this.table = joinedTable;
		return this;
	}

	/**
	 * @param joinAlias
	 *            the alias to give the JOINed table — used both as the SQL alias (qualifies columns) and as the alias
	 *            name remembered by {@link JooqSnowflakeSchemaBuilder} for subsequent {@code from(...)} calls.
	 * @return this
	 */
	public JooqJoinBuilder alias(String joinAlias) {
		this.alias = joinAlias;
		return this;
	}

	/**
	 * @param leftTableAlias
	 *            the alias of the LEFT table for this JOIN. Optional — defaults to the BASE table alias (the schema's
	 *            central fact table). Set this on snowflake legs where the LEFT side is a previously-joined dimension
	 *            rather than the fact, e.g. {@code .from("orders")} when joining {@code customer} onto {@code orders}.
	 * @return this
	 */
	public JooqJoinBuilder from(String leftTableAlias) {
		this.from = leftTableAlias;
		return this;
	}

	/**
	 * Add an ON clause comparing a LEFT-table column to a RIGHT-table column. Chainable — call multiple times for
	 * composite keys.
	 *
	 * @param leftColumn
	 *            the column on the LEFT table. Will be qualified by {@link #from(String)} when unqualified.
	 * @param rightColumn
	 *            the column on the joined table. Will be qualified by the join alias when unqualified.
	 * @return this
	 */
	public JooqJoinBuilder on(String leftColumn, String rightColumn) {
		on.add(Map.entry(leftColumn, rightColumn));
		return this;
	}

	/**
	 * Shortcut for {@link #on(String, String) on(column, column)} when both sides share the same column name (the
	 * common case for natural-key joins).
	 *
	 * @param column
	 *            the shared column name on both sides.
	 * @return this
	 */
	public JooqJoinBuilder onSame(String column) {
		return on(column, column);
	}

	/**
	 * Register a per-join column alias scoped to this JOIN's table — equivalent to calling
	 * {@link JooqSnowflakeSchemaBuilder#withAlias(String, String)} immediately after the JOIN, but kept inside the join
	 * definition for clearer locality.
	 *
	 * @param aliasName
	 *            the name a query may use to refer to the column.
	 * @param originalColumn
	 *            the column name on the JOINed table (unqualified — the join alias is added automatically).
	 * @return this
	 */
	public JooqJoinBuilder withAlias(String aliasName, String originalColumn) {
		columnAliases.put(aliasName, originalColumn);
		return this;
	}

	/**
	 * Explicit list of columns the joined table provides — honoured only by
	 * {@link PrunedJoinsJooqSnowflakeSchemaBuilder}, where it overrides the columns used for prunability decisions (see
	 * the original 5-arg {@code PrunedJoinsJooqSnowflakeSchemaBuilder#leftJoin(String, Table, String, List, Set)}).
	 *
	 * <p>
	 * On the base {@link JooqSnowflakeSchemaBuilder} this setter has no effect — the value is simply ignored.
	 *
	 * @param columns
	 *            the columns the joined table provides.
	 * @return this
	 */
	public JooqJoinBuilder providedColumns(Set<String> columns) {
		this.providedColumns = columns;
		return this;
	}

	// Package-private accessors used by JooqSnowflakeSchemaBuilder when committing the JOIN.

	void validate() {
		if (table == null) {
			throw new IllegalStateException("Missing table(...) on join builder");
		}
		if (alias == null) {
			throw new IllegalStateException("Missing alias(...) on join builder for table=" + table);
		}
		if (on.isEmpty()) {
			throw new IllegalStateException("Missing on(...) on join builder for alias=" + alias);
		}
	}
}
