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
package eu.solven.adhoc.measure.combination;

import java.util.List;
import java.util.Objects;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.sum.CoalesceAggregation;

/**
 * Return the first underlyingValue which is not null. Else null.
 * 
 * @author Benoit Lacelle
 */
public class CoalesceCombination implements ICombination {

	public static final String KEY = CoalesceAggregation.KEY;

	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		return underlyingValues.stream().filter(Objects::nonNull).findFirst().orElse(null);
	}

	/**
	 * 
	 * @param combination
	 * @return true if given combination is a {@link CoalesceCombination}.
	 */
	public static boolean isFindFirst(ICombination combination) {
		return combination instanceof CoalesceCombination;
	}

}
