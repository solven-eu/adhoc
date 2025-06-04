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
}
