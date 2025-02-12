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

import eu.solven.adhoc.query.filter.ColumnFilter;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.util.regex.Pattern;

/**
 * To be used with {@link ColumnFilter}, for regex-based matchers.
 *
 * @author Benoit Lacelle
 * @see LikeMatcher
 */
@Value
@Builder
@Jacksonized
public class RegexMatcher implements IValueMatcher {
	Pattern pattern;

	public static Pattern compile(final String regex) {
		// BEWARE how to customize flags?
		return Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
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
		return pattern.matcher(asCharSequence).matches();
	}

	public static IValueMatcher matching(String regex) {
		return RegexMatcher.builder().pattern(compile(regex)).build();
	}

	public static IValueMatcher matching(Pattern pattern) {
		return RegexMatcher.builder().pattern(pattern).build();
	}
}
