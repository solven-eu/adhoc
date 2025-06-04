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

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.ISliceAndValueConsumer;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.MultitypeNavigableMergeableColumn;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.map.AdhocMap;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Bucketor;
import eu.solven.adhoc.measure.operator.IOperatorsFactory;
import eu.solven.adhoc.measure.transformator.ATransformatorQueryStep;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.groupby.GroupByHelpers;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link ITransformatorQueryStep} for {@link Bucketor}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class BucketorQueryStep extends ATransformatorQueryStep implements ITransformatorQueryStep {
	final Bucketor bucketor;
	final IOperatorsFactory operatorsFactory;
	@Getter
	final CubeQueryStep step;

	final Supplier<ICombination> combinationSupplier = Suppliers.memoize(this::makeCombination);

	protected ICombination makeCombination() {
		return operatorsFactory.makeCombination(bucketor);
	}

	protected IAggregation getMakeAggregation() {
		return operatorsFactory.makeAggregation(bucketor);
	}

	public List<String> getUnderlyingNames() {
		return bucketor.getUnderlyings();
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		return getUnderlyingNames().stream().map(underlying -> {
			IAdhocGroupBy groupBy = GroupByHelpers.union(step.getGroupBy(), bucketor.getGroupBy());
			return CubeQueryStep.edit(step).groupBy(groupBy).measure(underlying).build();
		}).collect(Collectors.toList());
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.isEmpty()) {
			return SliceToValue.empty();
		}

		IAggregation agg = getMakeAggregation();

		IMultitypeMergeableColumn<SliceAsMap> aggregatingView = makeColumn(agg);

		ICombination combinator = combinationSupplier.get();

		forEachDistinctSlice(underlyings, combinator, aggregatingView::merge);

		return SliceToValue.builder().column(aggregatingView).build();
	}

	protected IMultitypeMergeableColumn<SliceAsMap> makeColumn(IAggregation agg) {
		return MultitypeNavigableMergeableColumn.<SliceAsMap>builder().aggregation(agg).build();
	}

	@Override
	protected void onSlice(List<? extends ISliceToValue> underlyings,
			SliceAndMeasures bucketedSlice,
			ICombination combinator,
			ISliceAndValueConsumer output) {
		try {
			IValueProvider valueProvider = combinator.combine(bucketedSlice.getSlice(), bucketedSlice.getMeasures());

			if (isDebug()) {
				log.info("[DEBUG] m={} c={} transformed {} into {} at {}",
						bucketor.getName(),
						bucketor.getCombinationKey(),
						bucketedSlice.getMeasures(),
						IValueProvider.getValue(valueProvider),
						bucketedSlice);
			}

			SliceAsMap outputCoordinate = queriedSlice(step.getGroupBy(), bucketedSlice.getSlice());

			if (isDebug()) {
				log.info("[DEBUG] m={} contributed {} into {}", bucketor.getName(), valueProvider, outputCoordinate);
			}

			valueProvider.acceptReceiver(output.putSlice(outputCoordinate));
		} catch (RuntimeException e) {
			List<?> underlyingVs = bucketedSlice.getMeasures().asList();
			throw new IllegalArgumentException(
					"Issue combining c=%s values=%s in bucketedSlice=%s"
							.formatted(combinator.getClass(), underlyingVs, bucketedSlice),
					e);
		}
	}

	protected SliceAsMap queriedSlice(IAdhocGroupBy queryGroupBy, ISliceWithStep bucketedSlice) {
		NavigableSet<String> groupedByColumns = queryGroupBy.getGroupedByColumns();

		AdhocMap.AdhocMapBuilder mapBuilder = AdhocMap.builder(groupedByColumns);

		groupedByColumns.forEach(groupBy -> {
			Object value = bucketedSlice.getRawSliced(groupBy);

			if (value == null) {
				// Should we accept null a coordinate, e.g. to handle input partial Maps?
				throw new IllegalStateException("A coordinate-value can not be null");
			}

			mapBuilder.append(value);
		});

		return SliceAsMap.fromMap(mapBuilder.build());
	}
}
