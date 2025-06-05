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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceAndValueConsumer;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.MultitypeHashColumn;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.engine.step.SliceAsMapWithStep;
import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.filter.editor.IFilterEditor.FilterEditorContext;
import eu.solven.adhoc.map.AdhocMap;
import eu.solven.adhoc.map.AdhocMap.AdhocMapBuilder;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.measure.operator.IOperatorsFactory;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link ITransformatorQueryStep} for {@link Shiftor}.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class ShiftorQueryStep implements ITransformatorQueryStep {
	final Shiftor shiftor;
	final IOperatorsFactory operatorsFactory;
	@Getter
	final CubeQueryStep step;

	final Supplier<IFilterEditor> filterEditorSupplier = Suppliers.memoize(this::makeFilterEditor);

	protected IFilterEditor makeFilterEditor() {
		return operatorsFactory.makeEditor(shiftor.getEditorKey(), shiftor.getEditorOptions());
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		// This will provide underlying values from the shifted slice
		String underlyingMeasure = shiftor.getUnderlying();

		IAdhocFilter shiftedFilter = shift(step.getFilter(), step.getCustomMarker());

		// Read values from the shifted underlyingStep
		CubeQueryStep whereToReadShifted =
				CubeQueryStep.edit(step).filter(shiftedFilter).measure(underlyingMeasure).build();
		// Read slices from the natural undelryingStep, as the natural slices to write
		CubeQueryStep whereToReadForWrite = CubeQueryStep.edit(step).measure(underlyingMeasure).build();

		// Query both querySteps, as they may not provide the same slices
		return Arrays.asList(whereToReadShifted, whereToReadForWrite);
	}

	protected IAdhocFilter shift(IAdhocFilter filter, Object customMarker) {
		FilterEditorContext filterEditorContext =
				IFilterEditor.FilterEditorContext.builder().filter(filter).customMarker(customMarker).build();
		return filterEditorSupplier.get().editFilter(filterEditorContext);
	}

	// @Override
	protected void onSlice(List<? extends ISliceToValue> underlyings,
			ISliceWithStep slice,
			ISliceAndValueConsumer output) {
		ISliceToValue whereToReadShifted = underlyings.getFirst();

		SliceAsMap shiftedSlice = shiftSlice(slice);

		// Read the value from the whereToReadShifted, on the slice recomputed from the whereToReadForWrite
		whereToReadShifted.onValue(shiftedSlice).acceptReceiver(output.putSlice(slice.getAdhocSliceAsMap()));
	}

	/**
	 * Given a whereToWrite slice, get the equivalent whereToRead slice.
	 *
	 * @param slice
	 * @return
	 */
	protected SliceAsMap shiftSlice(ISliceWithStep slice) {
		// BEWARE the filter from queryStep is meaningless here
		IAdhocFilter filter = slice.getAdhocSliceAsMap().asFilter();

		IAdhocFilter editedSlice = shift(filter, step.getCustomMarker());
		Map<String, Object> editedAsMap = FilterHelpers.asMap(editedSlice);

		AdhocMapBuilder builder = AdhocMap.builder(step.getGroupBy().getGroupedByColumns());

		step.getGroupBy().getGroupedByColumns().forEach(column -> {
			Object value = editedAsMap.get(column);
			if (value == null) {
				throw new IllegalStateException("Missing value for column=%s in %s".formatted(column, editedAsMap));
			}
			builder.append(value);
		});

		// Typically useful on querying grandTotal
		// if (!Sets.difference(editedAsMap.keySet(), step.getGroupBy().getGroupedByColumns()).isEmpty()) {
		// editedAsMap = new HashMap<>(editedAsMap);
		//
		// editedAsMap.keySet().retainAll(step.getGroupBy().getGroupedByColumns());
		// }

		return SliceAsMap.fromMap(builder.build());
	}

	protected boolean isDebug() {
		return getStep().isDebug();
	}

	protected void forEachDistinctSlice1(List<? extends ISliceToValue> underlyings, ISliceAndValueConsumer output) {
		forEachDistinctSlice2(underlyings, slice -> onSlice(underlyings, slice, output));
	}

	protected void forEachDistinctSlice2(List<? extends ISliceToValue> underlyings,
			Consumer<SliceAsMapWithStep> sliceConsumer) {
		ISliceToValue whereToReadForWrite = underlyings.getLast();

		AtomicInteger slicesDone = new AtomicInteger();
		whereToReadForWrite.forEachSlice(rawSlice -> {
			return v -> {
				// All we need is the slice, to know where to write
				log.debug("v={} is not used", v);

				SliceAsMapWithStep slice = SliceAsMapWithStep.builder().slice(rawSlice).queryStep(getStep()).build();

				try {
					sliceConsumer.accept(slice);
				} catch (RuntimeException e) {
					throw new IllegalArgumentException(
							"Issue processing m=%s slice=%s".formatted(shiftor.getName(), slice),
							e);
				}

				if (Integer.bitCount(slicesDone.incrementAndGet()) == 1 && isDebug()) {
					log.info("[DEBUG] Done processing {} slices", slicesDone);
				}
			};
		});
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.size() == 1) {
			log.debug("Happens when whereToRead matches whereToWrite");
		} else if (underlyings.size() != 2) {
			throw new IllegalArgumentException("underlyings.size() != 2");
		}

		IMultitypeColumnFastGet<SliceAsMap> storage = makeStorage();

		forEachDistinctSlice1(underlyings, storage::append);

		return SliceToValue.builder().column(storage).build();
	}

	protected IMultitypeColumnFastGet<SliceAsMap> makeStorage() {
		return MultitypeHashColumn.<SliceAsMap>builder().build();
	}
}
