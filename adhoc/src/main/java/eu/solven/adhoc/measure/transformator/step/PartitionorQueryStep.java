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
package eu.solven.adhoc.measure.transformator.step;

import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.column.Cuboid;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.dataframe.column.ISliceAndValueConsumer;
import eu.solven.adhoc.dataframe.join.SliceAndMeasures;
import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.engine.tabular.inducer.JavaStreamInducedEvaluator;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.transformator.AMeasureQueryStep;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByHelpers;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link IMeasureQueryStep} for {@link Partitionor}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class PartitionorQueryStep extends AMeasureQueryStep {
	final Partitionor partitionor;
	@Getter(AccessLevel.PROTECTED)
	final IAdhocFactories factories;
	@Getter
	final CubeQueryStep step;

	final Supplier<ICombination> combinationSupplier = Suppliers.memoize(this::makeCombination);

	protected ICombination makeCombination() {
		return factories.getOperatorFactory().makeCombination(partitionor);
	}

	protected IAggregation getMakeAggregation() {
		return factories.getOperatorFactory().makeAggregation(partitionor);
	}

	public List<String> getUnderlyingNames() {
		return partitionor.getUnderlyings();
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		// Given current step, we add missing columns to cover partitionor groupBy
		// This way, we try as much as possible not to break sorting when removing these additional columns
		IGroupBy groupBy = getUnderlyingGroupBy();
		return getUnderlyingNames().stream()
				.map(underlying -> step.toBuilder().groupBy(groupBy).measure(underlying).build())
				.toList();
	}

	protected IGroupBy getUnderlyingGroupBy() {
		return GroupByHelpers.union(step.getGroupBy(), partitionor.getGroupBy());
	}

	@Override
	public ICuboid produceOutputColumn(List<? extends ICuboid> underlyings) {
		if (underlyings.isEmpty()) {
			return Cuboid.empty();
		}

		IAggregation agg = getMakeAggregation();

		IMultitypeMergeableColumn<ISlice> values = makeColumn(agg, underlyings);

		ICombination combinator = combinationSupplier.get();

		forEachDistinctSlice(underlyings, combinator, values::merge);

		return Cuboid.forGroupBy(step).values(values).build();
	}

	protected IMultitypeMergeableColumn<ISlice> makeColumn(IAggregation agg, List<? extends ICuboid> underlyings) {
		boolean breakSorting = isBreakSorting();

		// BEWARE The output capacity is at most the sum of input capacity. But it is
		// generally much smaller. (e.g. We
		// may receive 100 different CCYs, but output a single value cross CCYs).
		int initialCapacity = CombinatorQueryStep.sumSizes(underlyings);
		if (breakSorting) {
			return factories.getColumnFactory().makeColumnRandomInsertions(agg, initialCapacity);
		} else {
			return factories.getColumnFactory().makeColumn(agg, initialCapacity);
		}

	}

	protected boolean isBreakSorting() {
		// TODO We should build GROUP BY with most beneficial order from the beginning, i.e. not rely on User
		// configuration.
		Set<String> inducedColumns = step.getGroupBy().getSequencedColumns();

		Set<String> inducerColumns = getUnderlyingGroupBy().getSequencedColumns();

		return JavaStreamInducedEvaluator.breakSorting(inducerColumns, inducedColumns);
	}

	@Override
	protected void onSlice(SliceAndMeasures contributionSlice, ICombination combinator, ISliceAndValueConsumer output) {
		try {
			IValueProvider valueProvider =
					combinator.combine(contributionSlice.getSlice(), contributionSlice.getMeasures());

			if (isDebug()) {
				log.info("[DEBUG] m={} c={} transformed {} into {} at {}",
						partitionor.getName(),
						partitionor.getCombinationKey(),
						contributionSlice.getMeasures(),
						IValueProvider.getValue(valueProvider),
						contributionSlice);
			}

			ISlice partitionSlice = queriedSlice(step.getGroupBy(), contributionSlice.getSlice());

			if (isDebug()) {
				log.info("[DEBUG] m={} contributed {} into {}",
						partitionor.getName(),
						IValueProvider.getValue(valueProvider),
						partitionSlice);
			}

			valueProvider.acceptReceiver(output.putSlice(partitionSlice));
		} catch (RuntimeException e) {
			List<?> underlyingVs = contributionSlice.getMeasures().asList();
			throw new IllegalArgumentException(
					"Issue combining c=%s values=%s in bucketedSlice=%s"
							.formatted(combinator.getClass(), underlyingVs, contributionSlice),
					e);
		}
	}

	protected ISlice queriedSlice(IGroupBy queryGroupBy, ISliceWithStep bucketedSlice) {
		NavigableSet<String> groupedByColumns = queryGroupBy.getSortedColumns();

		return bucketedSlice.getSlice().retainAll(groupedByColumns);
	}
}
