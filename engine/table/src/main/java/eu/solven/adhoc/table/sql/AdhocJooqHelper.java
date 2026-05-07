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
package eu.solven.adhoc.table.sql;

import java.util.function.Supplier;

import org.jooq.Name;
import org.jooq.Parser;
import org.jooq.impl.DSL;
import org.jooq.impl.ParserException;

import eu.solven.adhoc.query.ICountMeasuresConstants;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility methods to help coding with JooQ.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@UtilityClass
public class AdhocJooqHelper {

	public static void disableBanners() {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	/**
	 *
	 * @param columnName
	 * @return true if the columnName is actually an expression, meaning it is not a real column.
	 */
	public static boolean isExpression(String columnName) {
		if (ICountMeasuresConstants.ASTERISK.equals(columnName)) {
			// Typically on `COUNT(*)`
			return true;
		} else {
			return false;
		}
	}

	public static Name name(String name, Supplier<Parser> parserSupplier) {
		if (isExpression(name)) {
			// e.g. `name=*`
			return DSL.unquotedName(name);
		}

		Parser parser = parserSupplier.get();
		try {
			return parser.parseName(name)
					// quoted as we may receive `a@b@c` which has a special meaning in SQL
					.quotedName();
		} catch (ParserException e) {
			if (log.isDebugEnabled()) {
				log.debug("Issue parsing `{}` as name. Quoting it ", name, e);
			} else {
				log.debug("Issue parsing `{}` as name. Quoting it ", name);
			}
			// BEWARE This is certainly buggy
			return DSL.quotedName(name.split("\\."));
		}
	}

	/**
	 * Builds a qualified column name {@code <joinAlias>.<columnName>} suitable for use as a lookup key in a
	 * {@code Map<String, ?>} keyed on column references. When neither part contains a dot, the bare-dotted form
	 * {@code joinAlias + "." + columnName} is returned — the natural shape a query string typically uses (e.g.
	 * {@code groupByAlso("b.country")}). When either part contains a dot (column names like {@code "a.b"}, alias names
	 * like {@code "cust.x"}), the bare-dotted concatenation is ambiguous (does {@code "cust.x.a.b"} mean
	 * {@code "cust"."x.a.b"} or {@code "cust.x"."a.b"}?), so we fall back to the JOOQ-escaped two-part
	 * {@code Name.toString()} form which keeps each segment quoted independently.
	 *
	 * <p>
	 * Centralises the rule shared by callers that build qualified keys (e.g.
	 * {@code PrunedJoinsJooqTableSupplier.columnToAlias} and {@code JooqTableWrapper.getColumns} when renaming a
	 * shadowed column to its qualified form) so the dot-in-name edge case is handled in one place.
	 *
	 * @param joinAlias
	 *            the owning join's alias (the LHS of the qualifier)
	 * @param columnName
	 *            the column's bare name (the RHS)
	 * @return bare-dotted form when both parts are dot-free; JOOQ-escaped two-part name otherwise
	 */
	public static String qualifiedColumnName(String joinAlias, String columnName) {
		if (joinAlias.indexOf('.') < 0 && columnName.indexOf('.') < 0) {
			return joinAlias + "." + columnName;
		}
		return DSL.name(joinAlias, columnName).toString();
	}

	/**
	 * Returns the unqualified column name for a possibly-qualified, possibly-quoted input. Handles
	 * {@code "join"."col with space"} (jOOQ-escaped 2-part), {@code join.col} (bare-dotted), and {@code col} (no
	 * qualifier) — replaces the fragile {@code lastIndexOf('.') + substring} pattern that breaks on quoted identifiers
	 * (the leading quote bleeds into the substring).
	 *
	 * <p>
	 * Takes a parser supplier so the caller controls the dialect-aware quoting strategy. Pass the supplier from
	 * {@link JooqTableWrapperParameters#getDslSupplier()} or equivalent — using a default parser would risk
	 * misinterpreting identifiers under non-default dialects.
	 *
	 * @param qualifiedName
	 *            possibly-qualified, possibly-quoted name as produced by jOOQ's {@code Name.toString()}
	 * @param parserSupplier
	 *            dialect-aware parser supplier — same shape as {@link #name(String, Supplier)}
	 * @return the last segment as a clean unquoted string
	 */
	public static String unqualifiedColumnName(String qualifiedName, Supplier<Parser> parserSupplier) {
		return name(qualifiedName, parserSupplier).last();
	}
}
