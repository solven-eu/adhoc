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

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import eu.solven.adhoc.collection.AdhocCollectionHelpers;
import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.column.Cuboid;
import eu.solven.adhoc.dataframe.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.dataframe.column.ISliceAndValueConsumer;
import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.engine.step.SliceAsMapWithStep;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.filter.editor.IFilterEditor.FilterEditorContext;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.filter.value.InMatcher;
import eu.solven.adhoc.filter.value.NullMatcher;
import eu.solven.adhoc.map.factory.IMapBuilderPreKeys;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link IMeasureQueryStep} for {@link Shiftor}.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class ShiftorQueryStep implements IMeasureQueryStep {
	final Shiftor shiftor;
	final IAdhocFactories factories;
	@Getter
	final CubeQueryStep step;

	final Supplier<IFilterEditor> filterEditorSupplier = Suppliers.memoize(this::makeFilterEditor);

	protected IFilterEditor makeFilterEditor() {
		return factories.getOperatorFactory().makeEditor(shiftor.getEditorKey(), shiftor.getEditorOptions());
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		// This will provide underlying values from the shifted slice
		String underlyingMeasure = shiftor.getUnderlying();

		ISliceFilter shiftedFilter = shift(step.getFilter(), step.getCustomMarker());

		// Read values from the shifted underlyingStep
		CubeQueryStep whereToReadShifted =
				CubeQueryStep.edit(step).filter(shiftedFilter).measure(underlyingMeasure).build();
		// Read slices from any DB-materialized row via Aggregator.empty(), so a cell whose natural underlying has
		// no value (e.g. blue+JPY missing) but whose shifted slice has one (blue+EUR exists) still gets written.
		// See TestCubeQuery_Shiftor#testNotShiftMissingOnMeasure_ShiftedExist.
		CubeQueryStep whereToReadForWrite = CubeQueryStep.edit(step).measure(Aggregator.empty()).build();

		if (whereToReadShifted.equals(whereToReadForWrite)) {
			log.debug("whereToReadShited and whereToWrite are equals");
		}

		// Query both querySteps, as they may not provide the same slices
		return Arrays.asList(whereToReadShifted, whereToReadForWrite);
	}

	protected ISliceFilter shift(ISliceFilter filter, Object customMarker) {
		FilterEditorContext filterEditorContext =
				IFilterEditor.FilterEditorContext.builder().filter(filter).customMarker(customMarker).build();
		return filterEditorSupplier.get().editFilter(filterEditorContext);
	}

	/**
	 * Given a whereToWrite slice, get the equivalent whereToRead slice.
	 *
	 * @param slice
	 * @return
	 */
	protected ISlice shiftSlice(ISliceWithStep slice) {
		// BEWARE the filter from queryStep is meaningless here
		ISliceFilter filter = slice.getSlice().asFilter();

		ISliceFilter editedSlice = shift(filter, step.getCustomMarker());

		NavigableSet<String> columns = step.getGroupBy().getSortedColumns();
		IMapBuilderPreKeys builder = slice.getSlice().getFactory().newMapBuilder(columns);

		columns.forEach(column -> {
			Optional<?> optOperand = EqualsMatcher.extractOperand(FilterHelpers.getValueMatcher(editedSlice, column));

			Object value;
			if (optOperand.isEmpty()) {
				value = NullMatcher.NULL_HOLDER;
			} else {
				value = optOperand.get();
			}
			builder.append(value);
		});

		return builder.build().asSlice();
	}

	protected boolean isDebug() {
		return getStep().isDebug();
	}

	// @Override
	protected void onSlice(List<? extends ICuboid> underlyings, ISliceWithStep slice, ISliceAndValueConsumer output) {
		ICuboid whereToReadShifted = underlyings.getFirst();

		ISlice shiftedSlice = shiftSlice(slice);

		// Read the value from the whereToReadShifted, on the slice recomputed from the whereToReadForWrite
		IValueProvider shiftedValue = whereToReadShifted.onValue(shiftedSlice);
		shiftedValue.acceptReceiver(output.putSlice(slice.getSlice()));

		if (isDebug()) {
			log.info("[DEBUG] Write {}={} at {} (shifted from {} by {} to {})",
					shiftor.getName(),
					IValueProvider.getValue(shiftedValue),
					slice,
					shiftor.getUnderlying(),
					shiftor.getEditorKey(),
					shiftedSlice);
		}
	}

	protected void forEachDistinctSlice1(List<? extends ICuboid> underlyings, ISliceAndValueConsumer output) {
		forEachDistinctSlice2(underlyings, slice -> onSlice(underlyings, slice, output));
	}

	protected void forEachDistinctSlice2(List<? extends ICuboid> underlyings,
			Consumer<SliceAsMapWithStep> sliceConsumer) {
		ICuboid whereToReadForWrite = underlyings.getLast();

		AtomicInteger slicesDone = new AtomicInteger();

		// This second block will catch slices which does not exist in DB, but can be materialized given User filters
		processSlicesMaterializedByFilters(sliceConsumer, whereToReadForWrite, slicesDone);

		processSlicesMaterializedByTable(sliceConsumer, whereToReadForWrite, slicesDone);
	}

	protected void processSlicesMaterializedByTable(Consumer<SliceAsMapWithStep> sliceConsumer,
			ICuboid whereToReadForWrite,
			AtomicInteger slicesDone) {
		whereToReadForWrite.forEachSlice(rawSlice -> {
			return v -> {
				// All we need is the slice, to know where to write
				log.debug("v={} is not used", v);

				emitSlice(rawSlice, sliceConsumer, slicesDone);
			};
		});
	}

	/**
	 * Used to cover the case where a filter is explicitly referencing some coordinates, which would be fed by a
	 * Shiftor, even if the coordinate/cell is not actually present in the table.
	 * 
	 * @param sliceConsumer
	 * @param whereToReadForWrite
	 * @param slicesDone
	 */
	protected void processSlicesMaterializedByFilters(Consumer<SliceAsMapWithStep> sliceConsumer,
			ICuboid whereToReadForWrite,
			AtomicInteger slicesDone) {
		Set<String> columns = step.getGroupBy().getSequencedColumns();

		SetMultimap<String, Object> columnToFilteredValues =
				MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();

		columns.forEach(column -> {
			IValueMatcher valueMatcher = FilterHelpers.getValueMatcher(step.getFilter(), column);

			if (valueMatcher instanceof EqualsMatcher equalsMatcher) {
				columnToFilteredValues.put(column, equalsMatcher.getOperand());
			} else if (valueMatcher instanceof InMatcher inMatcher) {
				columnToFilteredValues.putAll(column, inMatcher.getOperands());
			} else {
				log.debug("No explicit values for column={}", column);
			}
		});

		// If any groupBy column has no pinned value, we cannot synthesize a slice for it: bail out.
		// `Sets.cartesianProduct` on an empty list returns a singleton containing an empty list, which
		// correctly yields one grandTotal slice when the groupBy itself is empty.
		boolean allPinned = columns.stream().allMatch(c -> !columnToFilteredValues.get(c).isEmpty());
		log.debug("[SHIFTOR] columns={} pinned={} allPinned={}", columns, columnToFilteredValues, allPinned);
		if (allPinned) {
			List<Set<Object>> perColumnValues =
					columns.stream().<Set<Object>>map(c -> ImmutableSet.copyOf(columnToFilteredValues.get(c))).toList();

			ISliceFactory sliceFactory = factories.getSliceFactory();

			BigInteger cartesianProductSize = AdhocCollectionHelpers.cartesianProductSize(perColumnValues);
			if (cartesianProductSize.compareTo(BigInteger.valueOf(AdhocUnsafe.cartesianProductLimit)) >= 0) {
				throw new IllegalArgumentException("Too-large cartesian product given columns=%s on set=%s"
						.formatted(columnToFilteredValues, step));
			}

			Sets.cartesianProduct(perColumnValues).forEach(combo -> {
				IMapBuilderPreKeys builder = sliceFactory.newMapBuilder(columns);
				combo.forEach(builder::append);
				ISlice rawSlice = builder.build().asSlice();

				Object value = IValueProvider.getValue(whereToReadForWrite.onValue(rawSlice));

				if (value == null) {
					log.debug("Registering a filter-synthesized slice={}", rawSlice);
					emitSlice(rawSlice, sliceConsumer, slicesDone);
				} else {
					log.debug("Already processed by table slice={}", rawSlice);
				}
			});
		}
	}

	/**
	 * Wrap {@code rawSlice} with the current {@link CubeQueryStep}, push it through {@code sliceConsumer}, and bump the
	 * {@code slicesDone} counter (logging at power-of-two milestones if debug is on). Shared between the normal
	 * forEachSlice branch and the user-filter-cartesian fallback so both report the same way.
	 */
	protected void emitSlice(ISlice rawSlice, Consumer<SliceAsMapWithStep> sliceConsumer, AtomicInteger slicesDone) {
		SliceAsMapWithStep slice = SliceAsMapWithStep.builder().slice(rawSlice).queryStep(getStep()).build();

		try {
			sliceConsumer.accept(slice);
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("Issue processing m=%s slice=%s".formatted(shiftor.getName(), slice), e);
		}

		if (Integer.bitCount(slicesDone.incrementAndGet()) == 1 && isDebug()) {
			log.info("[DEBUG] Done processing {} slices", slicesDone);
		}
	}

	@Override
	public ICuboid produceOutputColumn(List<? extends ICuboid> underlyings) {
		if (underlyings.size() == 1) {
			log.debug("Happens when whereToRead matches whereToWrite");
		} else if (underlyings.size() != 2) {
			throw new IllegalArgumentException("underlyings.size() == %s".formatted(underlyings.size()));
		}

		IMultitypeColumnFastGet<ISlice> values = makeColumn(underlyings);

		forEachDistinctSlice1(underlyings, values::append);

		return Cuboid.forGroupBy(step).values(values).build();
	}

	protected IMultitypeColumnFastGet<ISlice> makeColumn(List<? extends ICuboid> underlyings) {
		return factories.getColumnFactory()
				.makeColumn(p -> p.initialCapacity(ColumnatorQueryStep.sumSizes(underlyings)));
	}
}
