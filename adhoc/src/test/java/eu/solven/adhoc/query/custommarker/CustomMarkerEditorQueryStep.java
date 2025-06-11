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
package eu.solven.adhoc.query.custommarker;

import java.util.List;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceAndValueConsumer;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.CubeQueryStep.CubeQueryStepBuilder;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.engine.step.SliceAsMapWithStep;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CustomMarkerEditorQueryStep implements ITransformatorQueryStep {
	final CustomMarkerEditor customMarkerEditor;
	final CubeQueryStep step;

	public List<String> getUnderlyingNames() {
		return customMarkerEditor.getUnderlyingNames();
	}

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		CubeQueryStepBuilder stepBuilder =
				CubeQueryStep.edit(step).customMarker(customMarkerEditor.editCustomMarker(step.optCustomMarker()));

		return getUnderlyingNames().stream().map(underlyingName -> {
			return stepBuilder.measure(underlyingName).build();
		}).toList();
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.size() != 1) {
			throw new IllegalArgumentException("underlyingNames.size() != 1 (was %s)".formatted(underlyings.size()));
		}

		IMultitypeColumnFastGet<SliceAsMap> storage = makeStorage();

		ISliceToValue singleUnderlying = underlyings.getFirst();

		singleUnderlying.forEachSlice(rawSlice -> {
			SliceAsMapWithStep slice = SliceAsMapWithStep.builder().slice(rawSlice).queryStep(step).build();

			return v -> onSlice(slice, v, storage::append);
		});

		return SliceToValue.builder().column(storage).build();
	}

	private boolean isDebug() {
		return step.isDebug();
	}

	protected void onSlice(ISliceWithStep slice, Object value, ISliceAndValueConsumer output) {
		if (isDebug()) {
			log.info("[DEBUG] Write {} in {} for {}", value, slice, customMarkerEditor.getName());
		}

		output.putSlice(slice.getAdhocSliceAsMap(), value);
	}

	protected IMultitypeColumnFastGet<SliceAsMap> makeStorage() {
		return MultitypeHashColumn.<SliceAsMap>builder().build();
	}
}
