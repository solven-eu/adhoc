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

import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;
import eu.solven.adhoc.slice.ISliceWithStep;

/**
 * An {@link ICombination} can turn a {@link List} of values (typically from {@link Combinator}) into a new value. As a
 * {@link eu.solven.adhoc.measure.model.IMeasure}, it writes into current {@link eu.solven.adhoc.slice.IAdhocSlice}.
 *
 * @author Benoit Lacelle
 */
public interface ICombination {

	// TODO This shall become the optimal API, not to require to provide Objects
	default Object combine(SliceAndMeasures slice) {
		return combine(slice.getSlice(), slice.getMeasures().asList());
	}

	/**
	 * @param slice
	 * @param underlyingValues
	 *            the underlying measures values for current slice.
	 * @return the combined result at given given coordinate.
	 */
	default Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		// The simplest logic does not rely on the coordinates
		return combine(underlyingValues);
	}

	/**
	 * @param underlyingValues
	 *            the underlying measures values for current slice.
	 * @return the combined result at given given coordinate.
	 */
	default Object combine(List<?> underlyingValues) {
		throw new UnsupportedOperationException(
				"%s requires the slice to be provided".formatted(this.getClass().getName()));
	}
}
