/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.query.filter.value;

import java.util.regex.Pattern;

import eu.solven.adhoc.query.filter.ColumnFilter;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * To be used with {@link ColumnFilter}, for like-based matchers.
 *
 * @author Benoit Lacelle
 * @see RegexMatcher
 */
// https://www.w3schools.com/sql/sql_like.asp
// https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-LIKE
@Value
@Builder
@Jacksonized
public class LikeMatcher implements IValueMatcher, IColumnToString {
	String pattern;

	/**
	 * 
	 * @param likePattern
	 *            a LIKE pattern, like `az*t_`.
	 * @param inputToTest
	 * @return
	 */
	public static boolean like(final String likePattern, final CharSequence inputToTest) {
		Pattern p = asPattern(likePattern);
		return p.matcher(inputToTest).matches();
	}

	/**
	 *
	 * @param likePattern
	 * @return the {@link Pattern} equivalent to this LIKE expression
	 */
	// https://www.alibabacloud.com/blog/how-to-efficiently-implement-sql-like-syntax-in-java_600079
	// Is it missing % escaping? And quoting other chars (e.g. in a like-pattern `ab%.`).
	public static Pattern asPattern(final String likePattern) {
		String regexPattern = likePattern.replace("_", ".").replace("%", ".*?");
		return Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
	}

	@Override
	public boolean match(Object value) {
		// Are we fine turning `null` into `"null"`?
		CharSequence asCharSequence;
		if (value instanceof CharSequence cs) {
			asCharSequence = cs;
		} else {
			// BEWARE Should we require explicit cast, than casting ourselves?
			asCharSequence = String.valueOf(value);
		}
		return like(getPattern(), asCharSequence);
	}

	@Override
	public String toString() {
		return "LIKE '%s'".formatted(pattern);
	}

	/**
	 * 
	 * @param likePattern
	 *            a LIKE pattern, like `az*t_`.
	 * @return
	 */
	public static IValueMatcher matching(String likePattern) {
		return LikeMatcher.builder().pattern(likePattern).build();
	}

	@Override
	public String toString(String column, boolean negated) {
		if (negated) {
			return column + " NOT LIKE '" + pattern + "'";
		} else {
			return column + " LIKE '" + pattern + "'";
		}
	}
}
