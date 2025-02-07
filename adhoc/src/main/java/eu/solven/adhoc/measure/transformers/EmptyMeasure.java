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
package eu.solven.adhoc.measure.transformers;

import java.util.List;
import java.util.Set;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.storage.ISliceToValue;
import eu.solven.adhoc.storage.SliceToValue;
import lombok.Value;

/**
 * This is a technical measure, useful for edge-cases (e.g. not throwing when requesting an unknown measure).
 * 
 * @author Benoit Lacelle
 *
 */
@Value
public class EmptyMeasure implements IMeasure, IHasUnderlyingMeasures {
	String name;

	@Override
	public Set<String> getTags() {
		return Set.of("technical");
	}

	@Override
	public List<String> getUnderlyingNames() {
		return List.of();
	}

	@Override
	public IHasUnderlyingQuerySteps wrapNode(IOperatorsFactory transformationFactory, AdhocQueryStep adhocSubQuery) {
		return new IHasUnderlyingQuerySteps() {

			@Override
			public List<AdhocQueryStep> getUnderlyingSteps() {
				return List.of();
			}

			@Override
			public ISliceToValue produceOutputColumn(List<? extends ISliceToValue> underlyings) {
				if (!underlyings.isEmpty()) {
					throw new UnsupportedOperationException("Should not be called as we do not have any underlyings");
				}
				return SliceToValue.empty();
			}

		};
	}

}
