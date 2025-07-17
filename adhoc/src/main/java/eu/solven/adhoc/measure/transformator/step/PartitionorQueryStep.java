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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.base.Suppliers;

import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.ISliceAndValueConsumer;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.map.StandardSliceFactory.MapBuilderPreKeys;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.transformator.ATransformatorQueryStep;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.groupby.GroupByHelpers;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link ITransformatorQueryStep} for {@link Partitionor}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class PartitionorQueryStep extends ATransformatorQueryStep {
	final Partitionor partitionor;
	@Getter(AccessLevel.PROTECTED)
	final AdhocFactories factories;
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
		return getUnderlyingNames().stream().map(underlying -> {
			IAdhocGroupBy groupBy = GroupByHelpers.union(step.getGroupBy(), partitionor.getGroupBy());
			return CubeQueryStep.edit(step).groupBy(groupBy).measure(underlying).build();
		}).collect(Collectors.toList());
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.isEmpty()) {
			return SliceToValue.empty();
		}

		IAggregation agg = getMakeAggregation();

		IMultitypeMergeableColumn<SliceAsMap> values = makeColumn(agg, underlyings);

		ICombination combinator = combinationSupplier.get();

		forEachDistinctSlice(underlyings, combinator, values::merge);

		return SliceToValue.forGroupBy(step).values(values).build();
	}

	protected IMultitypeMergeableColumn<SliceAsMap> makeColumn(IAggregation agg,
			List<? extends ISliceToValue> underlyings) {
		// BEWARE The output capacity is at most the sum of input capacity. But it is generally much smaller. (e.g. We
		// may receive 100 different CCYs, but output a single value cross CCYs).
		int initialCapacity = CombinatorQueryStep.sumSizes(underlyings);
		return factories.getColumnFactory().makeColumn(agg, initialCapacity);
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

			SliceAsMap partitionSlice = queriedSlice(step.getGroupBy(), contributionSlice.getSlice());

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

	protected SliceAsMap queriedSlice(IAdhocGroupBy queryGroupBy, ISliceWithStep bucketedSlice) {
		NavigableSet<String> groupedByColumns = queryGroupBy.getGroupedByColumns();

		MapBuilderPreKeys mapBuilder = factories.getSliceFactory().newMapBuilder(groupedByColumns);

		groupedByColumns.forEach(groupBy -> {
			Object value = bucketedSlice.getRawSliced(groupBy);

			if (value == null) {
				// Should we accept null a coordinate, e.g. to handle input partial Maps?
				throw new IllegalStateException("A coordinate-value can not be null");
			}

			mapBuilder.append(value);
		});

		return mapBuilder.build().asSlice().asSliceAsMap();
	}
}
