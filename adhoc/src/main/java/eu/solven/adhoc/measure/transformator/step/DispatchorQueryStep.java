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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.ICalculatedColumn;
import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.cuboid.tabular.ITabularGroupByRecord;
import eu.solven.adhoc.dataframe.column.Cuboid;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.dataframe.column.ISliceAndValueConsumer;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashMergeableColumn;
import eu.solven.adhoc.dataframe.filter.FilterMatcher;
import eu.solven.adhoc.dataframe.join.SliceAndMeasures;
import eu.solven.adhoc.dataframe.row.TabularGroupByRecordOverMap;
import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.engine.step.IWhereGroupByQuery;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.value.NullMatcher;
import eu.solven.adhoc.map.AdhocMapHelpers;
import eu.solven.adhoc.map.factory.IMapBuilderPreKeys;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.decomposition.DecompositionHelpers;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.IDecompositionEntry;
import eu.solven.adhoc.measure.decomposition.IDecompositionFactory;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.transformator.AMeasureQueryStep;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.util.AdhocDebug;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link IMeasureQueryStep} for {@link Dispatchor}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class DispatchorQueryStep extends AMeasureQueryStep implements IMeasureQueryStep {
	public static final String P_UNDERLYINGS = "underlyings";

	final Dispatchor dispatchor;
	@Getter(AccessLevel.PROTECTED)
	final IAdhocFactories factories;

	@Getter
	final CubeQueryStep step;

	final Supplier<FilterMatcher> filterMatcherSupplier = Suppliers.memoize(() -> FilterMatcher.builder()
			.sliceFactory(getFactories().getSliceFactory())
			.filter(getStep().getFilter())
			.onMissingColumn(DecompositionHelpers.onMissingColumn())
			.build());

	public List<String> getUnderlyingNames() {
		return dispatchor.getUnderlyingNames();
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		IDecomposition decomposition = makeDecomposition(ImmutableList.of());

		List<IWhereGroupByQuery> measurelessSteps = decomposition.getUnderlyingSteps(step);

		if (isDebug()) {
			log.info("[DEBUG] {} underlyingSteps given step={}", measurelessSteps, step);
		}

		String underlyingMeasure = dispatchor.getUnderlying();
		return measurelessSteps.stream()
				.map(subStep -> CubeQueryStep.edit(subStep).measure(underlyingMeasure).build())
				.toList();

	}

	protected IDecomposition makeDecomposition(List<? extends ICuboid> underlyings) {
		Map<String, Object> options = new LinkedHashMap<>();

		options.putAll(dispatchor.getDecompositionOptions());

		IDecomposition decomposition =
				factories.getOperatorFactory().makeDecomposition(dispatchor.getDecompositionKey(), options);

		// We must not add underlyings as options, else it would corrupt cache mechanisms on decompositions
		if (decomposition instanceof IDecompositionFactory adjustWithSlices) {
			// Hence we provide underlyings in a later step, with a Factory-like pattern
			decomposition = adjustWithSlices.makeWithSlices(underlyings);
		}

		return decomposition;
	}

	@Override
	protected void onSlice(SliceAndMeasures slice, ICombination combination, ISliceAndValueConsumer output) {
		throw new UnsupportedOperationException(
				"Unclear how to refactor IDispatchor in AHasUnderlyingQuerySteps.onSlice");
	}

	@Override
	public ICuboid produceOutputColumn(List<? extends ICuboid> underlyings) {
		if (underlyings.isEmpty()) {
			return Cuboid.empty();
		} else if (underlyings.size() != 1) {
			throw new IllegalArgumentException("A dispatchor expects a single underlying");
		}

		IAggregation agg = factories.getOperatorFactory().makeAggregation(dispatchor.getAggregationKey());

		IMultitypeMergeableColumn<ISlice> values = makeColumn(agg);

		IDecomposition decomposition = makeDecomposition(underlyings);

		forEachDistinctSlice(underlyings, slice -> onSlice(underlyings, slice, decomposition, values));

		return Cuboid.forGroupBy(step).values(values).build();
	}

	protected IMultitypeMergeableColumn<ISlice> makeColumn(IAggregation agg) {
		// Not MultitypeNavigableColumn as decomposition will prevent writing slices in order.
		// BEWARE This should be reviewed, as some later IMeasure would expect to receive an ordered slices
		return MultitypeHashMergeableColumn.<ISlice>builder().aggregation(agg).build();
	}

	protected void onSlice(List<? extends ICuboid> underlyings,
			SliceAndMeasures slice,
			IDecomposition decomposition,
			IMultitypeMergeableColumn<ISlice> aggregatingView) {
		Object value = IValueProvider.getValue(slice.getMeasures().read(0));

		if (value == null) {
			// The underlying value is empty: nothing to dispatch
			return;
		}

		List<IDecompositionEntry> decomposed = decomposition.decompose(slice.getSlice(), value);

		// This may actually be a mis-design, as `.decompose` should not have returned groups if groups are in
		// filter.
		// IDecompositionMergingContext mergingContext = decomposition.makeMergingContext(slice);

		decomposed.forEach(decompositionEntry -> {
			Map<String, ?> fragmentCoordinate = decompositionEntry.getSlice();

			if (!isRelevant(fragmentCoordinate)) {
				if (isDebug()) {
					log.info("[DEBUG] Rejected the filtered decomposedEntry slice={}", fragmentCoordinate);
				} else {
					log.debug("[DEBUG] Rejected the filtered decomposedEntry slice={}", fragmentCoordinate);
				}
				return;
			}
			IValueProvider fragmentValueProvider = decompositionEntry.getValue();

			if (isDebug()) {
				log.info("[DEBUG] Contribute {}={} ({} generated decomposition={}) in {}",
						dispatchor.getName(),
						IValueProvider.getValue(fragmentValueProvider),
						dispatchor.getDecompositionKey(),
						decompositionEntry.getSlice(),
						slice);
			}

			// Build the actual fragment coordinate, given the groupedBy columns (as the decomposition may have
			// returned finer entries).
			ISlice outputSlice = queryGroupBy(step.getGroupBy(), slice.getSlice(), fragmentCoordinate);

			fragmentValueProvider.acceptReceiver(aggregatingView.merge(outputSlice));

			if (isDebug()) {
				aggregatingView.onValue(outputSlice)
						.acceptReceiver(o -> log.info("[DEBUG] slice={} has been merged into agg={}",
								fragmentCoordinate,
								AdhocDebug.toString(o)));
			}
		});
	}

	protected ISlice queryGroupBy(@NonNull IGroupBy groupBy, ISliceWithStep slice, Map<String, ?> fragmentCoordinate) {
		NavigableSet<String> groupByColumns = groupBy.getGroupedByColumns();
		IMapBuilderPreKeys queryCoordinatesBuilder = slice.getSlice().getFactory().newMapBuilder(groupByColumns);

		ISliceFactory sliceFactory = AdhocFactoriesUnsafe.factories.getSliceFactory();
		groupByColumns.forEach(groupByColumn -> {
			// BEWARE it is legal to get groupColumns only from the fragment coordinate
			Object value = fragmentCoordinate.get(groupByColumn);

			IAdhocColumn column = groupBy.getNameToColumn().get(groupByColumn);
			if (column instanceof ICalculatedColumn calculatedColumn) {
				Map<String, Object> sliceAsMap = new LinkedHashMap<>();
				sliceAsMap.putAll(slice.getSlice().asAdhocMap());

				if (value != null) {
					sliceAsMap.put(groupByColumn, value);
				}
				ISlice preSlice = AdhocMapHelpers.fromMap(sliceFactory, sliceAsMap).asSlice();
				ITabularGroupByRecord groupByRecord =
						TabularGroupByRecordOverMap.builder().groupBy(groupBy).slice(preSlice).build();
				Object calculatedCoordinate = calculatedColumn.computeCoordinate(groupByRecord);
				value = calculatedCoordinate;
			} else {
				if (value == null) {
					// Happens on groupBy along not-generated columns
					value = slice.sliceReader().extractCoordinateLax(groupByColumn, Object.class).orElse(null);
				}

				if (value == null) {
					value = NullMatcher.NULL_HOLDER;
					// Should we accept null a coordinate, e.g. to handle input partial Maps?
					// throw new IllegalStateException("A sliced-value can not be null
					// (column=%s)".formatted(groupByColumn));
				}
			}

			queryCoordinatesBuilder.append(value);
		});

		return queryCoordinatesBuilder.build().asSlice();
	}

	/**
	 * Some {@link IDecomposition} may provide irrelevant {@link IDecompositionEntry} given queryStep
	 * {@link ISliceFilter}.
	 * 
	 * @param decomposedSlice
	 * @return
	 */
	protected boolean isRelevant(Map<String, ?> decomposedSlice) {
		return filterMatcherSupplier.get().match(decomposedSlice);
	}
}
