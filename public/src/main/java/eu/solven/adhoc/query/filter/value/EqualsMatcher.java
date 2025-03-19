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

import java.util.function.Supplier;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Suppliers;

import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.resource.HasWrappedSerializer;
import lombok.Builder;
import lombok.EqualsAndHashCode;
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
@EqualsAndHashCode(exclude = { "operandIsLongLike", "operandIsDoubleLike" })
// BEWARE This is not a strict `equals`, as it has special rules around ints and longs
// We may introduce a StrictEqualsMatcher
// BEWARE Should we introduce a way to match primitive value without boxing?
public class EqualsMatcher implements IValueMatcher, IHasWrapped {
	@NonNull
	// @JsonValue
	Object operand;

	@JsonIgnore
	Supplier<Boolean> operandIsLongLike = Suppliers.memoize(() -> AdhocPrimitiveHelpers.isLongLike(getOperand()));
	@JsonIgnore
	Supplier<Boolean> operandIsDoubleLike = Suppliers.memoize(() -> AdhocPrimitiveHelpers.isDoubleLike(getOperand()));

	public Object getWrapped() {
		return operand;
	}

	@Override
	public boolean match(Object value) {
		if (operand == value || operand.equals(value)) {
			return true;
		}

		if (operandIsDoubleLike.get()) {
			if (operandIsLongLike.get()) {
				if (AdhocPrimitiveHelpers.isLongLike(value)) {
					// We consider `int 3` and `long 3` to be equals
					return AdhocPrimitiveHelpers.asLong(operand) == AdhocPrimitiveHelpers.asLong(value);
				}
			}

			if (AdhocPrimitiveHelpers.isDoubleLike(value)) {
				// We consider `float 1.2` and `double 1.2` to be equals
				return AdhocPrimitiveHelpers.asDouble(operand) == AdhocPrimitiveHelpers.asDouble(value);
			}
		}

		return false;
	}

	public static class EqualsMatcherBuilder {

		public EqualsMatcherBuilder() {
		}

		// Enable Jackson deserialization given a plain Object
		// https://github.com/FasterXML/jackson-databind/issues/5030
		// @JsonCreator
		public EqualsMatcherBuilder(Object operand) {

			this.operand(operand);
		}

		// https://github.com/FasterXML/jackson-databind/issues/5030
		// Enable Jackson deserialization given a plain String
		// TODO Does the need for an int constructor a Jackson bug?
		public EqualsMatcherBuilder(String operand) {
			this.operand(operand);
		}

		// https://github.com/FasterXML/jackson-databind/issues/5030
		// Enable Jackson deserialization given a plain String
		// TODO Does the need for a double constructor a Jackson bug?
		public EqualsMatcherBuilder(int operand) {
			this.operand(operand);
		}

		public EqualsMatcherBuilder(long operand) {
			this.operand(operand);
		}

		// https://github.com/FasterXML/jackson-databind/issues/5030
		// Enable Jackson deserialization given a plain String
		// TODO Does the need for a String constructor a Jackson bug?
		public EqualsMatcherBuilder(double operand) {
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
