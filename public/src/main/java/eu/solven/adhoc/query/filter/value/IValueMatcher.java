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

import java.util.Collection;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.pepper.core.PepperLogHelper;

// https://stackoverflow.com/questions/19379863/how-to-deserialize-interface-fields-using-jacksons-objectmapper
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
		include = JsonTypeInfo.As.PROPERTY,
		property = "type",
		defaultImpl = EqualsMatcher.class)
@JsonSubTypes({ @JsonSubTypes.Type(value = EqualsMatcher.class, name = "equals"),
		@JsonSubTypes.Type(value = SameMatcher.class, name = "same"),
		@JsonSubTypes.Type(value = InMatcher.class, name = "in"),
		@JsonSubTypes.Type(value = LikeMatcher.class, name = "like"),
		@JsonSubTypes.Type(value = RegexMatcher.class, name = "regex"),
		@JsonSubTypes.Type(value = NotMatcher.class, name = "not"),
		@JsonSubTypes.Type(value = NullMatcher.class, name = "null"),
		@JsonSubTypes.Type(value = AndMatcher.class, name = "and"),
		@JsonSubTypes.Type(value = OrMatcher.class, name = "or"),
		@JsonSubTypes.Type(value = ComparingMatcher.class, name = "compare") })
public interface IValueMatcher {
	IValueMatcher MATCH_ALL = AndMatcher.builder().build();
	IValueMatcher MATCH_NONE = OrMatcher.builder().build();

	/**
	 *
	 * @param value
	 *            may be null
	 * @return true if the value is matched
	 */
	boolean match(Object value);

	/**
	 * 
	 * @param matching
	 *            may be null, a {@link Collection}, a {@link IValueMatcher}, or any other value for a
	 *            {@link EqualsMatcher}.
	 * @return
	 */
	static IValueMatcher matching(Object matching) {
		if (matching == null) {
			return NullMatcher.matchNull();
		} else if (matching instanceof IValueMatcher vm) {
			return vm;
		} else if (matching instanceof Collection<?> c) {
			return InMatcher.isIn(c);
		} else if (matching instanceof IColumnFilter) {
			throw new IllegalArgumentException("Can not use a IColumnFilter as valueFilter: %s"
					.formatted(PepperLogHelper.getObjectAndClass(matching)));
		} else if (matching instanceof Pattern) {
			// May happen due to sick API in LikeMatcher
			throw new IllegalArgumentException(
					"Invalid matching: %s".formatted(PepperLogHelper.getObjectAndClass(matching)));
		} else {
			return EqualsMatcher.isEqualTo(matching);
		}
	}
}
