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
package eu.solven.adhoc.measure.transformator;

import java.util.List;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.storage.ISliceToValue;

/**
 * {@link IMeasure} which are not {@link Aggregator} defines underlying nodes and transform them together. (e.g. given
 * underlyings measures, or different coordinates).
 * 
 * @author Benoit Lacelle
 *
 */
public interface ITransformator {
	/**
	 * This is a {@link List} as in some edge-cases, a measure may refer multiple times the same underlyingSteps (e.g. a
	 * Filtrator on a slice which match the filter).
	 * 
	 * @return the {@link List} of underlying {@link AdhocQueryStep}
	 */
	List<AdhocQueryStep> getUnderlyingSteps();

	ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings);
}
