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
package eu.solven.adhoc.measure.sum;

import java.util.List;
import java.util.Map;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.pepper.mappath.MapPathGet;

/**
 * A {@link ICombination} which divide the 2 underlying measures. The first measure is used as numerator; the second
 * measure is used as denominator.
 * 
 * @author Benoit Lacelle
 */
// https://learn.microsoft.com/en-us/dax/divide-function-dax
public class DivideCombination implements ICombination {

	public static final String KEY = "DIVIDE";

	public static boolean isDivide(String operator) {
		return "/".equals(operator) || DivideCombination.KEY.equals(operator)
				|| operator.equals(DivideCombination.class.getName());
	}

	final boolean nullNumeratorIsZero;

	public DivideCombination() {
		nullNumeratorIsZero = false;
	}

	public DivideCombination(Map<String, ?> options) {
		nullNumeratorIsZero = MapPathGet.<Boolean>getOptionalAs(options, "nullNumeratorIsZero").orElse(false);
	}

	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		if (underlyingValues.size() != 2) {
			throw new IllegalArgumentException("Expected 2 underlyings. Got %s".formatted(underlyingValues.size()));
		}

		Object rawNumerator = underlyingValues.get(0);
		Object rawDenominator = underlyingValues.get(1);

		if (rawNumerator == null && rawDenominator == null) {
			return null;
		}

		if (rawNumerator == null && nullNumeratorIsZero) {
			rawNumerator = 0D;
		}

		if (rawNumerator instanceof Number numerator) {
			if (rawDenominator instanceof Number denominator) {
				return numerator.doubleValue() / denominator.doubleValue();
			} else {
				return Double.NaN;
			}
		} else if (rawNumerator == null) {
			if (rawDenominator instanceof Number) {
				// We may assimilate null to 0D: 0D / anything is 0D. Though, returning null if denominator is null is a
				// nice way to prevent Unfiltrator to materialize irrelevant slices.
				// BEWARE We may want a special behavior if denominator is also 0
				return null;
			} else {
				return Double.NaN;
			}
		} else {
			return Double.NaN;
		}
	}

}
