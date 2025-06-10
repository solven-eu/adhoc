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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Collectors;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.data.column.ISliceAndValueConsumer;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.hash.MultitypeHashMergeableColumn;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.map.AdhocMap;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.DecompositionHelpers;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.IDecompositionEntry;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.transformator.ATransformatorQueryStep;
import eu.solven.adhoc.measure.transformator.AdhocDebug;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.FilterMatcher;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link ITransformatorQueryStep} for {@link Dispatchor}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class DispatchorQueryStep extends ATransformatorQueryStep implements ITransformatorQueryStep {
	final Dispatchor dispatchor;
	@Getter(AccessLevel.PROTECTED)
	final AdhocFactories factories;

	@Getter
	final CubeQueryStep step;

	public List<String> getUnderlyingNames() {
		return dispatchor.getUnderlyingNames();
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		IDecomposition decomposition = makeDecomposition();

		List<IWhereGroupByQuery> measurelessSteps = decomposition.getUnderlyingSteps(step);

		if (isDebug()) {
			log.info("[DEBUG] {} underlyingSteps given step={}", measurelessSteps, step);
		}

		String underlyingMeasure = dispatchor.getUnderlying();
		return measurelessSteps.stream()
				.map(subStep -> CubeQueryStep.edit(subStep).measure(underlyingMeasure).build())
				.collect(Collectors.toList());

	}

	protected IDecomposition makeDecomposition() {
		return factories.getOperatorFactory()
				.makeDecomposition(dispatchor.getDecompositionKey(), dispatchor.getDecompositionOptions());
	}

	@Override
	protected void onSlice(SliceAndMeasures slice, ICombination combination, ISliceAndValueConsumer output) {
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

		IAggregation agg = factories.getOperatorFactory().makeAggregation(dispatchor.getAggregationKey());

		IMultitypeMergeableColumn<SliceAsMap> aggregatingView = makeColumn(agg);

		IDecomposition decomposition = makeDecomposition();

		forEachDistinctSlice(underlyings, slice -> onSlice(underlyings, slice, decomposition, aggregatingView));

		return SliceToValue.builder().column(aggregatingView).build();
	}

	protected IMultitypeMergeableColumn<SliceAsMap> makeColumn(IAggregation agg) {
		// Not MultitypeNavigableColumn as decomposition will prevent writing slices in order.
		// BEWARE This should be reviewed, as some later IMeasure would expect to receive an ordered slices
		return MultitypeHashMergeableColumn.<SliceAsMap>builder().aggregation(agg).build();
	}

	@Getter
	@RequiredArgsConstructor
	protected static class DecompositionMergingContext {
		final boolean isMultiGroupSlice;
		final Set<Map<String, ?>> outputCoordinatesAlreadyContributed;

		public static DecompositionMergingContext multiGroupSlice() {
			return new DecompositionMergingContext(true, new HashSet<>());
		}

		public static DecompositionMergingContext monoGroupSlice() {
			return new DecompositionMergingContext(false, Set.of());
		}

		public boolean addToSlice(Map<String, ?> outputCoordinate) {
			if (!isMultiGroupSlice) {
				// Not multiGroupSlice: the group is single and clearly stated
				return true;
			}

			// multiGroupSlice: ensure current element has not already been contributed
			return outputCoordinatesAlreadyContributed.add(outputCoordinate);
		}
	}

	protected void onSlice(List<? extends ISliceToValue> underlyings,
			SliceAndMeasures slice,
			IDecomposition decomposition,
			IMultitypeMergeableColumn<SliceAsMap> aggregatingView) {
		Object value = IValueProvider.getValue(slice.getMeasures().read(0));

		if (value == null) {
			// The underlying value is empty: nothing to dispatch
			return;
		}

		List<IDecompositionEntry> decomposed = decomposition.decompose(slice.getSlice(), value);

		// This may actually be a mis-design, as `.decompose` should not have returned groups if groups are in
		// filter.
		DecompositionMergingContext mergingContext = makeMergingContext(slice, decomposed);

		decomposed.forEach(decompositionEntry -> {
			Map<String, ?> fragmentCoordinate = decompositionEntry.getSlice();
			IValueProvider fragmentValueProvider = decompositionEntry.getValue();

			if (isDebug()) {
				Object fragmentValue = IValueProvider.getValue(fragmentValueProvider);
				log.info("[DEBUG] Contribute {} into {}", fragmentValue, fragmentCoordinate);
			}

			Map<String, ?> outputCoordinate = queryGroupBy(step.getGroupBy(), slice.getSlice(), fragmentCoordinate);

			if (!isRelevant(outputCoordinate)) {
				log.debug("Rejected the improper decomposedEntry slice={}", outputCoordinate);
				return;
			}

			if (mergingContext.addToSlice(outputCoordinate)) {
				SliceAsMap coordinateAsSlice = SliceAsMap.fromMap(outputCoordinate);
				fragmentValueProvider.acceptReceiver(aggregatingView.merge(coordinateAsSlice));

				if (isDebug()) {
					aggregatingView.onValue(coordinateAsSlice)
							.acceptReceiver(o -> log.info("[DEBUG] slice={} has been merged into agg={}",
									fragmentCoordinate,
									AdhocDebug.toString(o)));
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

	private DecompositionMergingContext makeMergingContext(SliceAndMeasures slice,
			List<IDecompositionEntry> decomposed) {
		DecompositionMergingContext mergingContext;
		// If current slice is contributing to multiple groups (e.g. a filter with an IN), we should accept each element
		// only once even if given element contributes to multiple matching groups. (e.g. if we look for `G8 or
		// G20`, `FR` should contributes only once).
		// NOTE This is a performance optimization, as we could rely on `outputCoordinatesAlreadyContributed`
		boolean isMultiGroupSlice;

		{
			NavigableSet<String> groupByColumns = slice.getSlice().getQueryStep().getGroupBy().getGroupedByColumns();
			if (decomposed.stream()
					.map(IDecompositionEntry::getSlice)
					.map(Map::keySet)
					.allMatch(groupByColumns::containsAll)) {
				// all decomposition columns are expressed in groupBy
				isMultiGroupSlice = false;
			} else {
				// TODO We may also manage the case where we filter a single group
				isMultiGroupSlice = true;
			}
		}

		if (isMultiGroupSlice) {
			mergingContext = DecompositionMergingContext.multiGroupSlice();
		} else {
			mergingContext = DecompositionMergingContext.monoGroupSlice();
		}
		return mergingContext;
	}

	protected Map<String, ?> queryGroupBy(@NonNull IAdhocGroupBy groupBy,
			ISliceWithStep slice,
			Map<String, ?> fragmentCoordinate) {
		NavigableSet<String> groupByColumns = groupBy.getGroupedByColumns();
		AdhocMap.AdhocMapBuilder queryCoordinatesBuilder = AdhocMap.builder(groupByColumns);

		groupByColumns.forEach(groupByColumn -> {
			// BEWARE it is legal to get groupColumns only from the fragment coordinate
			Object value = fragmentCoordinate.get(groupByColumn);

			if (value == null) {
				// BEWARE When would we get a groupBy from the slice rather than from the fragment coordinate?
				value = slice.getRawSliced(groupByColumn);
			}

			if (value == null) {
				// Should we accept null a coordinate, e.g. to handle input partial Maps?
				throw new IllegalStateException("A sliced-value can not be null");
			}

			queryCoordinatesBuilder.append(value);
		});

		return queryCoordinatesBuilder.build();
	}

	/**
	 * Some {@link IDecomposition} may provide irrelevant {@link IDecompositionEntry} given queryStep
	 * {@link IAdhocFilter}.
	 * 
	 * @param decomposedSlice
	 * @return
	 */
	protected boolean isRelevant(Map<String, ?> decomposedSlice) {
		return FilterMatcher.builder()
				.filter(step.getFilter())
				.onMissingColumn(DecompositionHelpers.onMissingColumn())
				.build()
				.match(decomposedSlice);
	}
}
