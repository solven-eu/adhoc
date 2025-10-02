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
package eu.solven.adhoc.query.filter.optimizer;

import java.util.Collection;

import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;

/**
 * Enable evaluating the complexity of a {@link ISliceFilter}. Typically used to choose a minimizing representation
 * amongst equivalent ones.
 * 
 * @author Benoit Lacelle
 */
public interface IFilterCostFunction {
	/**
	 * Lower cost is better.
	 * 
	 * @param filter
	 * @return
	 */
	long cost(ISliceFilter filter);

	long cost(IValueMatcher m);

	/**
	 * Evaluate the cost of an `AND` given its operands
	 * 
	 * @param operands
	 * @return the cost of given operands, considered as being AND together.
	 */
	default long cost(Collection<? extends ISliceFilter> operands) {
		// By default, the cost is additive through AND operands
		return operands.stream().mapToLong(this::cost).sum();
	}

}
