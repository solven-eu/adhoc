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
package eu.solven.adhoc.filter.editor;

import java.util.function.Function;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Value;

/**
 * An {@link IValueMatcher} useful to implement complex {@link IFilterEditor}.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Value
public class ShiftedValueMatcher implements IValueMatcher {

	IValueMatcher originalMatcher;
	Function<Object, ?> shifter;

	@Override
	public boolean match(Object value) {
		// TODO The shifted matcher should be built once and for all
		throw new NotYetImplementedException(
				"matcher=%s".formatted(PepperLogHelper.getObjectAndClass(originalMatcher)));
	}

	public static IValueMatcher shift(IValueMatcher originalMatcher, Function<Object, ?> shift) {
		if (originalMatcher instanceof EqualsMatcher equalsMatcher) {
			return EqualsMatcher.matchEq(shift.apply(equalsMatcher.getOperand()));
		} else if (originalMatcher instanceof InMatcher inMatcher) {
			return InMatcher.isIn(inMatcher.getOperands().stream().map(shift::apply).toList());
		} else {
			return ShiftedValueMatcher.builder().originalMatcher(originalMatcher).shifter(shift).build();
		}
	}
}
