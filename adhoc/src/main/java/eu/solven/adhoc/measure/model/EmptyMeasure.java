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
package eu.solven.adhoc.measure.model;

import java.util.List;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.With;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a technical measure, useful for edge-cases (e.g. not throwing when requesting an unknown measure).
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
@Slf4j
public class EmptyMeasure implements IMeasure, IHasUnderlyingMeasures {
	String name;

	@NonNull
	@Default
	@With
	ImmutableSet<String> tags = ImmutableSet.of("technical");

	@Override
	public List<String> getUnderlyingNames() {
		return List.of();
	}

	@Override
	public ITransformatorQueryStep wrapNode(AdhocFactories factories, CubeQueryStep queryStep) {
		return new ITransformatorQueryStep() {

			@Override
			public List<CubeQueryStep> getUnderlyingSteps() {
				return List.of();
			}

			@Override
			public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
				if (!underlyings.isEmpty()) {
					throw new IllegalArgumentException(
							"Should not be called as we do not have any underlyings. Received %s"
									.formatted(underlyings.size()));
				}
				return SliceToValue.empty();
			}

		};
	}

}
