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

import java.util.Map;

import org.jooq.TableLike;

import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.table.sql.join.PrunedJoinsJooqTableSupplierBuilder;

/**
 * Produces the jOOQ {@link TableLike} used in the {@code FROM} clause of a given {@link TableQueryV4}.
 * <p>
 * The default {@link JooqTableWrapper} path uses a single constant table supplied via
 * {@code JooqTableWrapperParameters.table}. Alternative implementations (e.g.
 * {@link PrunedJoinsJooqTableSupplierBuilder}) produce a query-specific {@link TableLike} — typically by pruning joins
 * that are not reachable from the columns actually referenced by the query.
 *
 * @author Benoit Lacelle
 */
public interface IJooqTableSupplier {

	/**
	 * @param tableQuery
	 *            the query about to be prepared
	 * @return the {@link TableLike} to place in the {@code FROM} clause for this query
	 */
	TableLike<?> tableFor(TableQueryV4 tableQuery);

	/**
	 * @return the {@link TableLike} used for schema introspection (i.e. {@code getColumns()} /
	 *         {@code getResultForFields()}). For pruning suppliers, this is the all-joins table; for the constant
	 *         supplier, it equals the table returned by {@link #tableFor(TableQueryV4)}.
	 */
	TableLike<?> getSchemaTable();

	/**
	 * Column-alias map: caller-facing name → qualified original column. Default is empty (no aliasing). Suppliers built
	 * from {@link PrunedJoinsJooqTableSupplierBuilder} (or any subclass of {@code JooqTableSupplierBuilder}) surface
	 * the schema's {@code aliasToOriginal} so {@link JooqTableWrapper#getColumns()} can attach the aliases to the
	 * underlying {@code ColumnMetadata}.
	 *
	 * @return alias → qualified-original mapping; empty when the supplier has no aliasing knowledge
	 */
	default Map<String, String> getAliasToOriginal() {
		return Map.of();
	}

	/**
	 * Column → owning join-alias index: lets consumers (e.g. {@link JooqTableWrapper#getColumns()}) figure out which
	 * join provides a given column name. Used to disambiguate when a caller-facing alias claims a bare name that's
	 * already owned by a different join — the shadowed column is then re-exposed under its qualified form.
	 *
	 * @return column-name → owning-join-alias mapping; empty when the supplier has no provenance knowledge.
	 *         Implementations may include both bare names and qualified forms; consumers should look up by bare name
	 *         only.
	 */
	default Map<String, String> getColumnToJoinAlias() {
		return Map.of();
	}

	/**
	 * Wraps a constant table as an {@link IJooqTableSupplier} that ignores the query.
	 */
	static IJooqTableSupplier constant(TableLike<?> table) {
		return new IJooqTableSupplier() {
			@Override
			public TableLike<?> tableFor(TableQueryV4 tableQuery) {
				return table;
			}

			@Override
			public TableLike<?> getSchemaTable() {
				return table;
			}
		};
	}
}
