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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.base.Suppliers;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Bucketor;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.groupby.GroupByHelpers;
import eu.solven.adhoc.slice.ISliceWithStep;
import eu.solven.adhoc.slice.SliceAsMap;
import eu.solven.adhoc.storage.ISliceAndValueConsumer;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.SliceToValue;
import eu.solven.adhoc.storage.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.storage.column.MultitypeHashMergeableColumn;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class BucketorQueryStep extends ATransformator implements ITransformator {
	final Bucketor bucketor;
	final IOperatorsFactory operatorsFactory;
	@Getter
	final AdhocQueryStep step;

	final Supplier<ICombination> combinationSupplier = Suppliers.memoize(this::makeCombination);

	protected ICombination makeCombination() {
		return operatorsFactory.makeCombination(bucketor);
	}

	protected IAggregation getMakeAggregation() {
		return operatorsFactory.makeAggregation(bucketor.getAggregationKey());
	}

	public List<String> getUnderlyingNames() {
		return bucketor.getUnderlyings();
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		return getUnderlyingNames().stream().map(underlying -> {
			IAdhocGroupBy groupBy = GroupByHelpers.union(step.getGroupBy(), bucketor.getGroupBy());
			return AdhocQueryStep.edit(step).groupBy(groupBy).measureNamed(underlying).build();
		}).collect(Collectors.toList());
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.isEmpty()) {
			return SliceToValue.empty();
		}

		IAggregation agg = getMakeAggregation();

		IMultitypeMergeableColumn<SliceAsMap> aggregatingView =
				MultitypeHashMergeableColumn.<SliceAsMap>builder().aggregation(agg).build();

		ICombination combinator = combinationSupplier.get();

		forEachDistinctSlice(underlyings, combinator, aggregatingView::merge);

		return SliceToValue.builder().column(aggregatingView).build();
	}

	@Override
	protected void onSlice(List<? extends ISliceToValue> underlyings,
			SliceAndMeasures slice,
			ICombination combinator,
			ISliceAndValueConsumer output) {
		List<?> underlyingVs = slice.getMeasures().asList();

		Object value;
		try {
			value = combinator.combine(slice.getSlice(), underlyingVs);
		} catch (RuntimeException e) {
			throw new IllegalArgumentException(
					"Issue combining c=%s values=%s in slice=%s".formatted(combinator.getClass(), underlyingVs, slice),
					e);
		}

		if (isDebug()) {
			List<String> underlyingNames = getUnderlyingNames();
			Map<String, Object> underylingVsAsMap = new TreeMap<>();

			for (int i = 0; i < underlyingNames.size(); i++) {
				underylingVsAsMap.put(underlyingNames.get(i), underlyingVs.get(i));
			}

			log.info("[DEBUG] m={} c={} transformed {} into {} at {}",
					bucketor.getName(),
					bucketor.getCombinationKey(),
					underylingVsAsMap,
					value,
					slice);
		}

		if (value != null) {
			Map<String, ?> outputCoordinate = queryGroupBy(step.getGroupBy(), slice.getSlice());

			if (isDebug()) {
				log.info("[DEBUG] m={} contributed {} into {}", bucketor.getName(), value, outputCoordinate);
			}

			output.putSlice(SliceAsMap.fromMap(outputCoordinate)).onObject(value);
		}
	}

	protected Map<String, ?> queryGroupBy(IAdhocGroupBy queryGroupBy, ISliceWithStep slice) {
		Map<String, Object> queryCoordinates = new HashMap<>();

		queryGroupBy.getGroupedByColumns().forEach(groupBy -> {
			Object value = slice.getRawSliced(groupBy);

			if (value == null) {
				// Should we accept null a coordinate, e.g. to handle input partial Maps?
				throw new IllegalStateException("A coordinate-value can not be null");
			}

			queryCoordinates.put(groupBy, value);
		});

		return queryCoordinates;
	}
}
