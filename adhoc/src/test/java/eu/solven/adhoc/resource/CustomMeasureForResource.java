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
package eu.solven.adhoc.resource;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.measure.IMeasure;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.step.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.step.IHasUnderlyingQuerySteps;

/**
 * Used to check the behavior of {@link MeasuresSetFromResource} on a project custom {@link IMeasure}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CustomMeasureForResource implements IMeasure, IHasUnderlyingMeasures {
	@JsonIgnore
	@Override
	public String getName() {
		return "someCustomName";
	}

	@JsonIgnore
	@Override
	public Set<String> getTags() {
		return Set.of("someCustomTag");
	}

	@Override
	public List<String> getUnderlyingNames() {
		return List.of();
	}

	@Override
	public IHasUnderlyingQuerySteps wrapNode(IOperatorsFactory transformationFactory, AdhocQueryStep adhocSubQuery) {
		return new EmptyHasUnderlyingQuerySteps();
	}
}
