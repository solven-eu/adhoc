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
package eu.solven.adhoc.engine.observability;

import java.util.List;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.factories.IAdhocFactories;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.transformator.ICombinator;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingNames;
import eu.solven.adhoc.measure.transformator.step.IMeasureQueryStep;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link IMeasureQueryStep} for {@link Combinator}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class ObservabilityTestQueryStep implements IMeasureQueryStep {
	final ICombinator combinator;

	@Getter(AccessLevel.PROTECTED)
	final IAdhocFactories factories;

	@Getter
	final CubeQueryStep step;

	final Supplier<ICombination> combinationSupplier = Suppliers.memoize(this::makeCombination);

	protected ICombination makeCombination() {
		return factories.getOperatorFactory().makeCombination(combinator);
	}

	public List<String> getUnderlyingNames() {
		ICombination combination = combinationSupplier.get();
		if (combination instanceof IHasUnderlyingNames hasUnderlyingNames) {
			// Happens on some ICombination, like those parsing an expression
			return hasUnderlyingNames.getUnderlyingNames();
		} else {
			return combinator.getUnderlyingNames();
		}
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		List<String> names = getUnderlyingNames();

		if (names.isEmpty()) {
			// This measure has no explicit underlyings: We add an implicit EmptyAggregator: it will materialize
			// the slices with no aggregate
			return ImmutableList.of(CubeQueryStep.edit(step).measure(Aggregator.empty()).build());
		}

		return names.stream()
				// Change the requested measureName to the underlying measureName
				.map(underlyingName -> CubeQueryStep.edit(step).measure(underlyingName).build())
				.toList();
	}

	@Override
	public ICuboid produceOutputColumn(List<? extends ICuboid> underlyings) {
		throw new UnsupportedOperationException("Not called for DAG explaining");
	}

}
