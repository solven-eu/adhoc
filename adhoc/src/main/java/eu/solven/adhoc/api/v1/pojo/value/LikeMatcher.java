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
package eu.solven.adhoc.api.v1.pojo.value;

import java.util.regex.Pattern;

import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * To be used with {@link ColumnFilter}, for regex-based matchers.
 *
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
public class LikeMatcher implements IValueMatcher {
	String like;

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
	// Is it missing % escaping?
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
		return LikeMatcher.like(getLike(), asCharSequence);
	}

	public static IValueMatcher matching(String likeExpression) {
		return LikeMatcher.builder().like(likeExpression).build();
	}
}
