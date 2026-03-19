/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.engine.tabular.inducer;

import java.util.Optional;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Aggregator;

/**
 * Defines the contract for evaluating an induced {@link CubeQueryStep} into an aggregated result column.
 *
 * <p>
 * Infrastructure dependencies (e.g. column factories, filter optimizers) are injected at construction time via an
 * {@link IInducedEvaluatorFactory}, keeping per-call parameters focused on the query context.
 *
 * <p>
 * An implementation is free to return {@link Optional#empty()} to signal it cannot handle a particular request,
 * allowing the caller to try the next strategy in a chain (see {@link ChainedInducedEvaluator}).
 *
 * @author Benoit Lacelle
 */
@FunctionalInterface
public interface IInducedEvaluator {

	/**
	 * Try to evaluate the induced step.
	 *
	 * @param inducerValues
	 *            the data from the inducer step
	 * @param inducer
	 *            the inducer {@link CubeQueryStep}
	 * @param induced
	 *            the induced {@link CubeQueryStep}
	 * @param leftoverFilter
	 *            the additional filter to apply on top of the inducer data
	 * @param aggregation
	 *            the {@link IAggregation} for the output column
	 * @param aggregator
	 *            the {@link Aggregator} describing the aggregation key
	 * @return {@link Optional#empty()} when this strategy cannot handle the request; otherwise the computed column.
	 */
	Optional<IMultitypeMergeableColumn<ISlice>> tryEvaluate(ICuboid inducerValues,
			CubeQueryStep inducer,
			CubeQueryStep induced,
			ISliceFilter leftoverFilter,
			IAggregation aggregation,
			Aggregator aggregator);
}
