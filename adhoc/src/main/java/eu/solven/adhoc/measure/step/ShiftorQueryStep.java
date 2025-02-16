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
package eu.solven.adhoc.measure.step;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.slice.AdhocSliceAsMapWithStep;
import eu.solven.adhoc.slice.IAdhocSliceWithStep;
import eu.solven.adhoc.storage.ISliceAndValueConsumer;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.SliceToValue;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link IHasUnderlyingQuerySteps} for {@link Shiftor}.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class ShiftorQueryStep implements IHasUnderlyingQuerySteps {
	final Shiftor shiftor;
	final IOperatorsFactory operatorsFactory;
	@Getter
	final AdhocQueryStep step;

	final Supplier<IFilterEditor> filterEditorSupplier = Suppliers.memoize(this::makeFilterEditor);

	protected IFilterEditor makeFilterEditor() {
		return operatorsFactory.makeEditor(shiftor.getEditorKey(), shiftor.getEditorOptions());
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		// This will provide underlying values from the shifted slice
		ReferencedMeasure underlyingMeasure = ReferencedMeasure.ref(shiftor.getUnderlying());
		AdhocQueryStep whereToRead = AdhocQueryStep.edit(step)
				.filter(shift(step.getFilter(), step.getCustomMarker()))
				.measure(underlyingMeasure)
				.build();
		AdhocQueryStep whereToWrite = AdhocQueryStep.edit(step).measure(underlyingMeasure).build();

		// Query both querySteps, as they may not provide the same slices
		return Arrays.asList(whereToRead, whereToWrite);
	}

	protected IAdhocFilter shift(IAdhocFilter filter, Object customMarker) {
		return filterEditorSupplier.get()
				.editFilter(
						IFilterEditor.FilterEditorContext.builder().filter(filter).customMarker(customMarker).build());
	}

	// @Override
	protected void onSlice(List<? extends ISliceToValue> underlyings,
			IAdhocSliceWithStep slice,
			ISliceAndValueConsumer output) {
		ISliceToValue whereToRead = underlyings.getFirst();

		AdhocSliceAsMap shiftedSlice = shiftSlice(slice);

		// Read the value from the whereToRead, on the slice recomputed from the whereToWrite
		whereToRead.onValue(shiftedSlice, value -> {
			output.putSlice(slice.getAdhocSliceAsMap(), value);
		});
	}

	/**
	 * Given a whereToWrite slice, get the equivalent whereToRead slice.
	 *
	 * @param slice
	 * @return
	 */
	protected AdhocSliceAsMap shiftSlice(IAdhocSliceWithStep slice) {
		IAdhocFilter editedSlice = shift(slice.asFilter(), step.getCustomMarker());
		Map<String, Object> editedAsMap = FilterHelpers.asMap(editedSlice);

		// Typically useful on querying grandTotal
		if (!Sets.difference(editedAsMap.keySet(), step.getGroupBy().getGroupedByColumns()).isEmpty()) {
			editedAsMap = new HashMap<>(editedAsMap);

			editedAsMap.keySet().retainAll(step.getGroupBy().getGroupedByColumns());
		}

		return AdhocSliceAsMap.fromMap(editedAsMap);
	}

	protected boolean isDebug() {
		return shiftor.isDebug() || getStep().isDebug();
	}

	protected Iterable<? extends AdhocSliceAsMap> distinctSlices(List<? extends ISliceToValue> underlyings) {
		return UnderlyingQueryStepHelpers.distinctSlices(isDebug(), underlyings);
	}

	protected void forEachDistinctSlice(List<? extends ISliceToValue> underlyings, ISliceAndValueConsumer output) {
		forEachDistinctSlice(underlyings, slice -> onSlice(underlyings, slice, output));
	}

	protected void forEachDistinctSlice(List<? extends ISliceToValue> underlyings,
			Consumer<AdhocSliceAsMapWithStep> sliceConsumer) {
		ISliceToValue whereToWrite = underlyings.getLast();
		Iterable<? extends AdhocSliceAsMap> distinctSlices = whereToWrite.slicesSet();

		int slicesDone = 0;
		for (AdhocSliceAsMap coordinates : distinctSlices) {
			AdhocSliceAsMapWithStep slice =
					AdhocSliceAsMapWithStep.builder().slice(coordinates).queryStep(getStep()).build();

			try {
				sliceConsumer.accept(slice);
			} catch (RuntimeException e) {
				throw new IllegalArgumentException("Issue processing m=%s slice=%s".formatted(shiftor.getName(), slice),
						e);
			}

			if (Integer.bitCount(++slicesDone) == 1) {
				if (isDebug()) {
					log.info("[DEBUG] Done processing {} slices", slicesDone);
				}
			}
		}
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.size() == 1) {
			log.debug("Happens when whereToRead matches whereToWrite");
		} else if (underlyings.size() != 2) {
			throw new IllegalArgumentException("underlyings.size() != 2");
		}

		ISliceToValue output = makeCoordinateToValues();

		forEachDistinctSlice(underlyings, output);

		return output;
	}

	protected ISliceToValue makeCoordinateToValues() {
		return SliceToValue.builder().build();
	}
}
