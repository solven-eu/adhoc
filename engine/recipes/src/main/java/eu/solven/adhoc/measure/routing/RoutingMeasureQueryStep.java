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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.data.cell.ProxyValueReceiver;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.dataframe.column.Cuboid;
import eu.solven.adhoc.dataframe.column.IAppendOnlyMultitypeColumn;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.ISliceAndValueConsumer;
import eu.solven.adhoc.dataframe.join.SliceAndMeasures;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.factories.IAdhocFactories;
import eu.solven.adhoc.factories.IColumnFactory;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.transformator.AMeasureQueryStep;
import eu.solven.adhoc.measure.transformator.step.CombinatorQueryStep;
import eu.solven.adhoc.model.measure.IMeasure;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Runtime counterpart of {@link RoutingMeasure}. Invokes the spec's {@link IRoutingLogic} once per
 * {@link CubeQueryStep} to obtain the list of underlying steps, then coalesces their per-slice values.
 *
 * <p>
 * Coalesce (rather than SUM) is hardcoded because the disjoint-decomposition pattern routing was introduced for has
 * "one slice ↔ one sub-step" as its contract: the cross-step combine is structural, not arithmetic. Hardcoding coalesce
 * surfaces accidental filter overlap as a "first wins" anomaly rather than a silently doubled value. If different
 * combine semantics are needed, a different measure type is the right tool.
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

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		IRoutingLogic logic = measure.getRoutingLogic();
		if (logic == null) {
			throw new IllegalStateException(
					"RoutingMeasure '%s' has no routingLogic set; this typically happens on a spec-only instance "
							.concat("(e.g. obtained via Jackson deserialization). step=%s")
							.formatted(measure.getName(), step));
		}
		List<CubeQueryStep> steps = logic.route(step);
		if (steps == null) {
			throw new IllegalStateException(
					"RoutingMeasure '%s': routingLogic returned null. Expected a (possibly empty) list of CubeQueryStep. step=%s"
							.formatted(measure.getName(), step));
		}
		if (steps.isEmpty()) {
			// Empty route is legitimate: the routing logic determined no underlying applies for this step.
			// `produceOutputColumn` will yield an empty cuboid in that case.
			return steps;
		}
		// The `underlyings` list is the covering set used by docs/dependency-graph utilities;
		// enforcing it at runtime keeps the two views consistent.
		for (CubeQueryStep returned : steps) {
			IMeasure m = returned.getMeasure();
			String mName = m.getName();
			if (!measure.getUnderlyings().contains(mName)) {
				throw new IllegalStateException(("RoutingMeasure '%s': routingLogic returned a step targeting measure"
						+ " '%s', not in declared underlyings %s. step=%s")
								.formatted(measure.getName(), mName, measure.getUnderlyings(), step));
			}
		}
		return steps;
	}

	@Override
	public ICuboid produceOutputColumn(List<? extends ICuboid> underlyings) {
		if (underlyings.isEmpty()) {
			return Cuboid.empty();
		}

		ICombination coalesce =
				factories.getOperatorFactory().makeCombination(CoalesceCombination.KEY, Collections.emptyMap());

		// Coalesce shortcut: with a single underlying, coalesce of one value is that value, so the underlying
		// cuboid can be returned as-is. Mirrors `CombinatorQueryStep`'s short-circuit for the same reason.
		if (CoalesceCombination.isFindFirst(coalesce) && underlyings.size() == 1) {
			return underlyings.getFirst();
		}

		IMultitypeColumnFastGet<ISlice> values =
				factories.getColumnFactory().makeColumn(p -> p.initialCapacity(IColumnFactory.sumSizes(underlyings)));

		ISliceAndValueConsumer output;
		if (values instanceof IAppendOnlyMultitypeColumn appendOnly) {
			output = appendOnly::appendNew;
		} else {
			output = values::append;
		}
		forEachDistinctSlice(underlyings, coalesce, output);

		return Cuboid.forGroupBy(step).values(values).build();
	}

	@Override
	protected void onSlice(SliceAndMeasures slice, ICombination combination, ISliceAndValueConsumer output) {
		ISlicedRecord slicedRecord = slice.getMeasures();
		IValueReceiver outputSlice = output.putSlice(slice.getSlice().getSlice());
		combine(slice.getSlice(), combination, slicedRecord, outputSlice);
	}

	/**
	 * Mirrors {@link CombinatorQueryStep#combine(ISliceWithStep, ICombination, ISlicedRecord, IValueReceiver)} — see
	 * that method for the rationale on the {@link ProxyValueReceiver} wrap. The pattern probably belongs in
	 * {@link AMeasureQueryStep} as a shared helper; until that refactor lands, the two copies must stay in sync.
	 */
	protected void combine(ISliceWithStep slice,
			ICombination combination,
			ISlicedRecord slicedRecord,
			IValueReceiver outputSlice) {
		if (isDebug()) {
			ProxyValueReceiver proxyReceiver = new ProxyValueReceiver(outputSlice);
			combination.combine(slice, slicedRecord, proxyReceiver);

			log.info("[DEBUG] Route {}={} (over {}) in {}",
					measure.getName(),
					IValueProvider.getValue(proxyReceiver.asValueProvider()),
					Objects.toString(slicedRecord),
					slice);
		} else {
			combination.combine(slice, slicedRecord, outputSlice);
		}
	}
}
