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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects;

import eu.solven.adhoc.query.filter.ColumnFilter;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * To be used with {@link ColumnFilter}, for IN-based matchers.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
public class InMatcher implements IValueMatcher {
	@NonNull
	Set<?> operands;

	@Override
	public boolean match(Object value) {
		if (operands.contains(value)) {
			return true;
		}

		if (operands.stream().anyMatch(operand -> operand instanceof IValueMatcher vm && vm.match(value))) {
			return true;
		}

		return false;
	}

	@Override
	public String toString() {
		MoreObjects.ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", operands.size());

		AtomicInteger index = new AtomicInteger();
		operands.stream().limit(128).forEach(filter -> {
			toStringHelper.add("#" + index.getAndIncrement(), filter);
		});

		return toStringHelper.toString();
	}

	public static IValueMatcher isIn(Set<?> allowedValues) {
		if (allowedValues.size() == 1) {
			Object singleValue = allowedValues.iterator().next();
			return EqualsMatcher.builder().operand(singleValue).build();
		} else {
			return InMatcher.builder().operands(allowedValues).build();
		}
	}

	public static IValueMatcher isIn(Object... operands) {
		return isIn(Set.of(operands));
	}
}
