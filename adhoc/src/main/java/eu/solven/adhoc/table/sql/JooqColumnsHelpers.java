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

import org.jooq.Field;
import org.jooq.TableLike;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.table.sql.join.PrunedJoinsJooqSnowflakeSchemaBuilder;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.NonNull;
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
	 * @param dslSupplier
	 *            JDBC backend used to run the probe query
	 * @return a fresh {@link IJooqColumnsResolver} running {@code SELECT * FROM <table> LIMIT 0} per lookup. Probe
	 *         results are cached upstream by {@link PrunedJoinsJooqSnowflakeSchemaBuilder} and by
	 *         {@link JooqTableWrapper}; this resolver itself performs no caching.
	 */
	public static IJooqColumnsResolver dbProbe(IDSLSupplier dslSupplier) {
		return new DbProbeResolver(dslSupplier);
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
		public List<Field<?>> columnsOf(TableLike<?> table) {
			return ImmutableList.copyOf(table.asTable().fields());
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
		@NonNull
		private final IDSLSupplier dslSupplier;

		@Override
		public List<Field<?>> columnsOf(TableLike<?> table) {
			// Log in INFO as the round-trip may be slow on large-schema JDBC drivers.
			log.info("Fetching fields via SELECT * LIMIT 0 of table={}",
					PepperLogHelper
							.lazyToString(() -> table.toString().replaceAll("\r", "\\r").replaceAll("\n", "\\n")));
			return ImmutableList.copyOf(dslSupplier.getDSLContext().select().from(table).limit(0).fetch().fields());
		}
	}
}
