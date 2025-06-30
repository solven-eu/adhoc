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

import eu.solven.adhoc.measure.combination.ICombination;

/**
 * An {@link ICombination} which sums underlyings.
 * 
 * @author Benoit Lacelle
 */
public class SumCombination extends AggregationCombination {
	public static final String KEY = SumAggregation.KEY;

	/**
	 * 
	 * @param nullOperandIsNull
	 *            if an underlying is null, we return {@link AggregationCombination#oneUnderlyingIsNull()}
	 */
	public SumCombination(boolean nullOperandIsNull) {
		super(new SumAggregation(), nullOperandIsNull);
	}

	/**
	 * By default, null are ignored.
	 */
	public SumCombination() {
		this(DEFAULT_CUSTOM_IF_ANY_NULL);
	}

}
