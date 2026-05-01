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
package eu.solven.adhoc.table.sql;

import java.util.List;
import java.util.function.Function;

import org.jooq.Field;
import org.jooq.TableLike;

import eu.solven.adhoc.table.sql.join.PrunedJoinsJooqTableSupplierBuilder;
import eu.solven.adhoc.util.Blocking;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory helpers for {@link IJooqColumnsResolver}. Two strategies are provided:
 * <ul>
 * <li>{@link #fromJooqFields()} — reads {@code table.asTable().fields()} directly. Works for code-generated or
 * {@code VALUES}-based tables.</li>
 * <li>{@link #dbProbe(IDSLSupplier)} — runs a {@code SELECT * LIMIT 0} probe through an {@link IDSLSupplier}. Required
 * when the jOOQ {@link TableLike} has no declared fields (typical for plain {@code DSL.table(Name)}).</li>
 * </ul>
 *
 * @author Benoit Lacelle
 */
@UtilityClass
public final class JooqColumnsHelpers {

	/**
	 * @return a shared {@link IJooqColumnsResolver} that reads {@code table.asTable().fields()} — fast, in-memory, and
	 *         sufficient when the jOOQ {@link TableLike} declares its fields (code-gen, {@code VALUES}, …).
	 */
	public static IJooqColumnsResolver fromJooqFields() {
		return JooqFieldsResolver.INSTANCE;
	}

	/**
	 * @return a fresh {@link IJooqColumnsResolver} running {@code SELECT * FROM <table> LIMIT 0} per lookup. Probe
	 *         results are cached upstream by {@link PrunedJoinsJooqTableSupplierBuilder} and by
	 *         {@link JooqTableWrapper}; this resolver itself performs no caching.
	 */
	public static IJooqColumnsResolver dbProbe() {
		return new DbProbeResolver();
	}

	/**
	 * @param sqlBuilder
	 *            given the queried {@link TableLike}, returns the raw SQL string to execute. Typically a single-row
	 *            probe such as {@code "SELECT * FROM \"dev\".\"public\".\"%s\" LIMIT 0".formatted(someTable)}.
	 * @return a fresh {@link IJooqColumnsResolver} that runs the caller-supplied raw SQL via the
	 *         {@link IDSLSupplier#getDSLContext()} {@code fetch(...)} entry-point and returns the result-set columns.
	 *         Useful for backends where the JOOQ-rendered probe would be wrong: e.g. Redshift accessed via the
	 *         PostgreSQL 8 dialect, where {@code LIMIT 0} interacts badly with some JOOQ-generated qualifiers, or when
	 *         the probe must hit a specific {@code database.schema.table} qualifier that JOOQ refuses to render.
	 */
	public static IJooqColumnsResolver predefinedSql(Function<TableLike<?>, String> sqlBuilder) {
		return new PredefinedSqlResolver(sqlBuilder);
	}

	/**
	 * Stateless {@link IJooqColumnsResolver} that reads {@link TableLike#asTable()} fields. Exposed via
	 * {@link #fromJooqFields()}.
	 */
	static final class JooqFieldsResolver implements IJooqColumnsResolver {
		static final JooqFieldsResolver INSTANCE = new JooqFieldsResolver();

		private JooqFieldsResolver() {
		}

		@Override
		public List<Field<?>> columnsOf(IDSLSupplier dslSupplier, TableLike<?> table) {
			return List.of(table.asTable().fields());
		}
	}

	/**
	 * {@link IJooqColumnsResolver} running a {@code SELECT * LIMIT 0} probe through the supplied {@link IDSLSupplier}.
	 * Used as the default resolver by {@link JooqTableWrapper} when
	 * {@link JooqTableWrapperParameters#getColumnsResolver()} is not configured.
	 */
	@RequiredArgsConstructor
	@Slf4j
	static final class DbProbeResolver implements IJooqColumnsResolver {

		@Blocking
		@Override
		public List<Field<?>> columnsOf(IDSLSupplier dslSupplier, TableLike<?> table) {
			// Log in INFO as the round-trip may be slow on large-schema JDBC drivers.
			log.info("Fetching fields via SELECT * LIMIT 0 of table={}",
					PepperLogHelper
							.lazyToString(() -> table.toString().replaceAll("\r", "\\r").replaceAll("\n", "\\n")));
			return List.of(dslSupplier.getDSLContext().select().from(table).limit(0).fetch().fields());
		}
	}

	/**
	 * {@link IJooqColumnsResolver} that runs a caller-supplied raw SQL probe — bypasses JOOQ's query builder. Used as
	 * an escape hatch for backends where JOOQ produces an SQL that the backend rejects (Redshift accessed via the
	 * PostgreSQL 8 dialect, etc.). Exposed via {@link #predefinedSql(Function)}.
	 */
	@RequiredArgsConstructor
	@Slf4j
	static final class PredefinedSqlResolver implements IJooqColumnsResolver {
		private final Function<TableLike<?>, String> sqlBuilder;

		@Blocking
		@Override
		public List<Field<?>> columnsOf(IDSLSupplier dslSupplier, TableLike<?> table) {
			String sql = sqlBuilder.apply(table);
			log.info("Fetching fields via predefined SQL={}",
					PepperLogHelper.lazyToString(() -> sql.replaceAll("\r", "\\r").replaceAll("\n", "\\n")));
			return List.of(dslSupplier.getDSLContext().fetch(sql).fields());
		}
	}
}
