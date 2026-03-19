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

import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import lombok.Builder;
import lombok.Singular;

/**
 * An {@link IInducedEvaluator} that delegates to a prioritised list of evaluators and returns the result of the first
 * one that succeeds (i.e. returns a non-empty {@link Optional}).
 *
 * <p>
 * Typical usage puts fast, specialised evaluators (e.g. {@link DuckDBInducedEvaluator}) first, with a universal
 * fallback (e.g. {@link JavaStreamInducedEvaluator}) last.
 *
 * @author Benoit Lacelle
 */
@Builder
public class ChainedInducedEvaluator implements IInducedEvaluator {

	@Singular
	final List<IInducedEvaluator> evaluators;

	@Override
	public Optional<IMultitypeMergeableColumn<ISlice>> tryEvaluate(ICuboid inducerValues,
			CubeQueryStep inducer,
			CubeQueryStep induced,
			ISliceFilter leftoverFilter,
			IAggregation aggregation,
			Aggregator aggregator) {
		for (IInducedEvaluator evaluator : evaluators) {
			Optional<IMultitypeMergeableColumn<ISlice>> result =
					evaluator.tryEvaluate(inducerValues, inducer, induced, leftoverFilter, aggregation, aggregator);
			if (result.isPresent()) {
				return result;
			}
		}
		return Optional.empty();
	}

	/**
	 * Convenience factory that builds a chain from an ordered list of evaluators.
	 *
	 * @param evaluators
	 *            ordered list; earlier entries have higher priority
	 * @return a new {@link ChainedInducedEvaluator}
	 */
	public static ChainedInducedEvaluator of(IInducedEvaluator... evaluators) {
		return ChainedInducedEvaluator.builder().evaluators(ImmutableList.copyOf(evaluators)).build();
	}
}
