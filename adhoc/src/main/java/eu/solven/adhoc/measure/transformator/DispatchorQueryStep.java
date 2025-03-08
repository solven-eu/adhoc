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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Collectors;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.map.AdhocMap;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.slice.ISliceWithStep;
import eu.solven.adhoc.slice.SliceAsMap;
import eu.solven.adhoc.storage.ISliceAndValueConsumer;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.SliceToValue;
import eu.solven.adhoc.storage.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.storage.column.MultitypeHashMergeableColumn;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class DispatchorQueryStep extends ATransformator implements ITransformator {
	final Dispatchor dispatchor;
	final IOperatorsFactory transformationFactory;

	@Getter
	final AdhocQueryStep step;

	public List<String> getUnderlyingNames() {
		return dispatchor.getUnderlyingNames();
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		IDecomposition decomposition = makeDecomposition();

		List<IWhereGroupbyAdhocQuery> measurelessSteps = decomposition.getUnderlyingSteps(step);

		if (isDebug()) {
			log.info("[DEBUG] {} underlyingSteps given step={}", measurelessSteps, step);
		}

		String underlyingMeasure = dispatchor.getUnderlying();
		return measurelessSteps.stream()
				.map(subStep -> AdhocQueryStep.edit(subStep).measureNamed(underlyingMeasure).build())
				.collect(Collectors.toList());

	}

	protected IDecomposition makeDecomposition() {
		return transformationFactory.makeDecomposition(dispatchor.getDecompositionKey(),
				dispatchor.getDecompositionOptions());
	}

	@Override
	protected void onSlice(List<? extends ISliceToValue> underlyings,
			SliceAndMeasures slice,
			ICombination combination,
			ISliceAndValueConsumer output) {
		throw new UnsupportedOperationException(
				"Unclear how to refactor IDispator in AHasUnderlyingQuerySteps.onSlice");
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.isEmpty()) {
			return SliceToValue.empty();
		} else if (underlyings.size() != 1) {
			throw new IllegalArgumentException("A dispatchor expects a single underlying");
		}

		IAggregation agg = transformationFactory.makeAggregation(dispatchor.getAggregationKey());

		IMultitypeMergeableColumn<SliceAsMap> aggregatingView =
				MultitypeHashMergeableColumn.<SliceAsMap>builder().aggregation(agg).build();

		IDecomposition decomposition = makeDecomposition();

		forEachDistinctSlice(underlyings, slice -> onSlice(underlyings, slice, decomposition, aggregatingView));

		return SliceToValue.builder().column(aggregatingView).build();
	}

	protected void onSlice(List<? extends ISliceToValue> underlyings,
			SliceAndMeasures slice,
			IDecomposition decomposition,
			IMultitypeMergeableColumn<SliceAsMap> aggregatingView) {
		List<?> underlyingVs = slice.getMeasures().asList();

		Object value = underlyingVs.getFirst();

		if (value != null) {
			Map<Map<String, ?>, Object> decomposed = decomposition.decompose(slice.getSlice(), value);

			// If current slice is holding multiple groups (e.g. a filter with an IN), we should accept each element
			// only once even if given element contributes to multiple matching groups. (e.g. if we look for `G8 or
			// G20`, `FR` should contributes only once).
			boolean isMultiGroupSlice;

			{
				Set<Set<String>> decompositionGroupBys =
						decomposed.keySet().stream().map(Map::keySet).collect(Collectors.toSet());

				NavigableSet<String> groupByColumns =
						slice.getSlice().getQueryStep().getGroupBy().getGroupedByColumns();
				if (decompositionGroupBys.stream().allMatch(groupByColumns::containsAll)) {
					// all group columns are expressed in groupBy
					isMultiGroupSlice = false;
				} else {
					// TODO We may also manage the case where we filter a single group
					isMultiGroupSlice = true;
				}

			}

			// This may actually be a mis-design, as `.decompose` should not have returned groups if groups are in
			// filter.
			Set<Map<String, ?>> outputCoordinatesAlreadyContributed = new HashSet<>();

			decomposed.forEach((fragmentCoordinate, fragmentValue) -> {
				if (isDebug()) {
					log.info("[DEBUG] Contribute {} into {}", fragmentValue, fragmentCoordinate);
				}

				Map<String, ?> outputCoordinate = queryGroupBy(step.getGroupBy(), slice.getSlice(), fragmentCoordinate);

				if (
				// Not multiGroupSlice: the group is single and clearly stated
				!isMultiGroupSlice
						// multiGroupSlice: ensure current element has not already been contributed
						|| outputCoordinatesAlreadyContributed.add(outputCoordinate)) {
					SliceAsMap coordinateAsSlice = SliceAsMap.fromMap(outputCoordinate);
					aggregatingView.merge(coordinateAsSlice).onObject(fragmentValue);

					if (isDebug()) {
						aggregatingView.onValue(coordinateAsSlice,
								o -> log.info("[DEBUG] slice={} has been merged into agg={}", fragmentCoordinate, o));
					}
				} else {
					// Typically happens on a multi-filter on the group hierarchy: a single element appears multiple
					// times (for each contributed group). but the element should be counted only once.
					// BEWARE Full discard is probably not-satisfying with a weighted-decomposition, as we have no
					// reason to keep. Though, a multi-filter on groups would be an unclear case.
					// One weighted instead of another. But it is OK for many2many as all groups have the same weight.
					log.debug("slice={} has already contributed into {}", slice, outputCoordinate);
				}
			});
		}
	}

	protected Map<String, ?> queryGroupBy(@NonNull IAdhocGroupBy queryGroupBy,
			ISliceWithStep slice,
			Map<String, ?> fragmentCoordinate) {
		AdhocMap.AdhocMapBuilder queryCoordinatesBuilder = AdhocMap.builder(queryGroupBy.getGroupedByColumns());

		queryGroupBy.getGroupedByColumns().forEach(groupBy -> {
			// BEWARE it is legal to get groupColumns only from the fragment coordinate
			Object value = fragmentCoordinate.get(groupBy);

			if (value == null) {
				// BEWARE When would we get a groupBy from the slice rather than from the fragment coordinate?
				value = slice.getRawSliced(groupBy);
			}

			if (value == null) {
				// Should we accept null a coordinate, e.g. to handle input partial Maps?
				throw new IllegalStateException("A sliced-value can not be null");
			}

			queryCoordinatesBuilder.append(value);
		});

		return queryCoordinatesBuilder.build();
	}
}
