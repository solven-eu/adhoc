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

import eu.solven.adhoc.table.sql.join.PrunedJoinsJooqSnowflakeSchemaBuilder;

/**
 * Resolves the columns of a jOOQ {@link TableLike} — name and type carried by each {@link Field}.
 * <p>
 * Two strategies cover most cases:
 * <ul>
 * <li>Reading {@code table.asTable().fields()} — works for code-generated or {@code VALUES}-based tables.</li>
 * <li>Probing the DB via {@code SELECT * LIMIT 0} — necessary for plain {@code DSL.table(Name)} whose fields jOOQ
 * cannot introspect. See {@link PrunedJoinsJooqSnowflakeSchemaBuilder#dslProbeResolver(IDSLSupplier)}.</li>
 * </ul>
 * Shared by {@link JooqTableWrapper} (to populate {@code getColumns()} schema introspection) and
 * {@link PrunedJoinsJooqSnowflakeSchemaBuilder} (to decide which joins can be pruned from the {@code FROM} clause).
 * Configuring the resolver on {@link JooqTableWrapperParameters#getColumnsResolver()} makes both paths pick up the same
 * customisation.
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IJooqColumnsResolver {

	/**
	 * @param table
	 *            the jOOQ {@link TableLike} whose columns are being queried
	 * @return the list of jOOQ {@link Field}s provided by {@code table}; may be empty if the columns cannot be
	 *         determined
	 */
	List<Field<?>> columnsOf(TableLike<?> table);
}
