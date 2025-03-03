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

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.measure.transformator.ITransformator;
import eu.solven.adhoc.slice.AdhocSliceAsMapWithStep;
import eu.solven.adhoc.slice.IAdhocSliceWithStep;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.SliceToValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CustomMarkerEditorQueryStep implements ITransformator {
	final CustomMarkerEditor customMarkerEditor;
	final AdhocQueryStep step;

	public List<String> getUnderlyingNames() {
		return customMarkerEditor.getUnderlyingNames();
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		return getUnderlyingNames().stream().map(underlyingName -> {
			return AdhocQueryStep.edit(step)
					.measureNamed(underlyingName)
					.customMarker(customMarkerEditor.editCustomMarker(step.optCustomMarker()))
					.build();
		}).toList();
	}

	@Override
	public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
		if (underlyings.size() != 1) {
			throw new IllegalArgumentException("underlyingNames.size() != 1");
		}

		ISliceToValue output = makeCoordinateToValues();

		ISliceToValue singleUnderlying = underlyings.getFirst();

		singleUnderlying.forEachSlice(rawSlice -> {
			AdhocSliceAsMapWithStep slice = AdhocSliceAsMapWithStep.builder().slice(rawSlice).queryStep(step).build();

			return v -> onSlice(slice, v, output);
		});

		return output;
	}

	private boolean isDebug() {
		return customMarkerEditor.isDebug() || step.isDebug();
	}

	protected void onSlice(IAdhocSliceWithStep slice, Object value, ISliceToValue output) {
		if (isDebug()) {
			log.info("[DEBUG] Write {} in {} for {}", value, slice, customMarkerEditor.getName());
		}

		output.putSlice(slice.getAdhocSliceAsMap(), value);
	}

	protected ISliceToValue makeCoordinateToValues() {
		return SliceToValue.builder().build();
	}
}
