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
package eu.solven.adhoc.measure.routing;

import java.util.List;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.column.Cuboid;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.ISliceAndValueConsumer;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.dataframe.join.SliceAndMeasures;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.factories.IAdhocFactories;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.transformator.AMeasureQueryStep;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Runtime counterpart of {@link RoutingMeasure}. Invokes the spec's {@code routeFunction} once per
 * {@link CubeQueryStep} to pick a single underlying measure, then passes that underlying's per-slice values through
 * unchanged.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class RoutingMeasureQueryStep extends AMeasureQueryStep {
	final RoutingMeasure measure;
	@Getter(AccessLevel.PROTECTED)
	final IAdhocFactories factories;

	@Getter
	final CubeQueryStep step;

	public List<String> getUnderlyingNames() {
		return measure.getUnderlyingNames();
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		String chosen = measure.getRouteFunction().apply(step);
		if (chosen == null) {
			throw new IllegalStateException(
					"RoutingMeasure '%s': routeFunction returned null. Expected one of %s. step=%s"
							.formatted(measure.getName(), measure.getUnderlyings(), step));
		}
		if (!measure.getUnderlyings().contains(chosen)) {
			throw new IllegalStateException(
					"RoutingMeasure '%s': routeFunction returned '%s', not in declared underlyings %s. step=%s"
							.formatted(measure.getName(), chosen, measure.getUnderlyings(), step));
		}
		return List.of(CubeQueryStep.edit(step).measure(chosen).build());
	}

	@Override
	public ICuboid produceOutputColumn(List<? extends ICuboid> underlyings) {
		if (underlyings.size() != 1) {
			throw new IllegalArgumentException("RoutingMeasure '%s': expected 1 underlying (the routed branch). Got %s"
					.formatted(measure.getName(), underlyings.size()));
		}

		IMultitypeColumnFastGet<ISlice> values = makeStorage();

		// The combination is required by AMeasureQueryStep but never functionally invoked here:
		// onSlice ignores it and emits the underlying value verbatim. See RoutingMeasure#getCombinationKey.
		ICombination passthroughCombination = factories.getOperatorFactory().makeCombination(measure);

		forEachDistinctSlice(underlyings, passthroughCombination, values::append);

		return Cuboid.forGroupBy(step).values(values).build();
	}

	@Override
	protected void onSlice(SliceAndMeasures slice, ICombination combination, ISliceAndValueConsumer output) {
		// Single-underlying passthrough: take the routed branch's value for this slice as-is.
		Object value = slice.getMeasures().asList().get(0);

		if (isDebug()) {
			log.info("[DEBUG] Route {} → {} for {}", measure.getName(), value, slice);
		}

		output.putSlice(slice.getSlice().getSlice()).onObject(value);
	}

	protected IMultitypeColumnFastGet<ISlice> makeStorage() {
		return MultitypeHashColumn.<ISlice>builder().build();
	}
}
