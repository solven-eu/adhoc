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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.resource.HasWrappedSerializer;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * To be used with {@link ColumnFilter}, for equality-based matchers.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
@JsonSerialize(using = HasWrappedSerializer.class)
public class EqualsMatcher implements IValueMatcher, IHasWrapped {
	@NonNull
	Object operand;

	public Object getWrapped() {
		return operand;
	}

	@Override
	public boolean match(Object value) {
		return operand == value || operand.equals(value);
	}

	public static class EqualsMatcherBuilder {

		public EqualsMatcherBuilder() {
		}

		// Enable Jackson deserialization given a plain Object
		public EqualsMatcherBuilder(Object operand) {

			this.operand(operand);
		}

		// Enable Jackson deserialization given a plain String
		// TODO Does the nead for a String constructor a Jackson bug?
		public EqualsMatcherBuilder(String operand) {

			this.operand(operand);
		}
	}

	public static IValueMatcher isEqualTo(Object operand) {
		if (operand == null) {
			return NullMatcher.matchNull();
		} else {
			return EqualsMatcher.builder().operand(operand).build();
		}
	}
}
