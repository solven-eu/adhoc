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
package eu.solven.adhoc.aggregations;

import java.util.List;

// https://learn.microsoft.com/en-us/dax/divide-function-dax
public class DivideCombination implements ICombination {

	public static final String KEY = "DIVIDE";

	@Override
	public Object combine(List<?> underlyingValues) {
		if (underlyingValues.size() != 2) {
			throw new IllegalArgumentException("Expected 2 underlyings. Got %s".formatted(underlyingValues.size()));
		}

		if (underlyingValues.get(0) instanceof Number numerator) {
			if (underlyingValues.get(1) instanceof Number denominator) {
				return numerator.doubleValue() / denominator.doubleValue();
			} else {
				return Double.NaN;
			}
		} else {
			return Double.NaN;
		}
	}

}
