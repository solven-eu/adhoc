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
package eu.solven.adhoc.engine.step;

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;

/**
 * An {@link IAdhocSlice} combined with an {@link CubeQueryStep}. It is useful to provide more contact to
 * {@link eu.solven.adhoc.measure.model.IMeasure}.
 * 
 * @author Benoit Lacelle
 */
public interface ISliceWithStep {
	IAdhocSlice getSlice();

	/**
	 * 
	 * @return the queryStep owning this slice. The slice should express only the groupBy in the queryStep.
	 */
	CubeQueryStep getQueryStep();

	/**
	 *
	 * @return an {@link IAdhocFilter} equivalent to this slice. It is never `matchNone`. It is always equivalent to a
	 *         {@link AndFilter} of {@link EqualsMatcher}.
	 */
	IAdhocFilter asFilter();

}
