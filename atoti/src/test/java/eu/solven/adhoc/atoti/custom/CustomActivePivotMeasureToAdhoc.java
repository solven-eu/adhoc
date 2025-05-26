/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.atoti.custom;

import eu.solven.adhoc.atoti.convertion.AtotiMeasureToAdhoc;
import lombok.Builder;
import lombok.experimental.SuperBuilder;

/**
 * Demonstrate how to extends {@link AtotiMeasureToAdhoc} given it has a {@link Builder}
 */
@SuperBuilder
public class CustomActivePivotMeasureToAdhoc extends AtotiMeasureToAdhoc {
	// @Builder(builderMethodName = "customBuilder")
	// public CustomActivePivotMeasureToAdhoc(ITableTranscoder transcoder, SourceMode sourceMode) {
	// super(new CustomAtotiConditionCubeToAdhoc(), transcoder, sourceMode);
	// }

	public static CustomActivePivotMeasureToAdhoc.CustomActivePivotMeasureToAdhocBuilder<?, ?> builder() {
		return new CustomActivePivotMeasureToAdhoc.CustomActivePivotMeasureToAdhocBuilderImpl()
				.apConditionToAdhoc(new CustomAtotiConditionCubeToAdhoc());
	}
}
