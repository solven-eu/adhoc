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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.ITransformator;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

/**
 * Used to check the behavior of {@link MeasureForestFromResource} on a project custom {@link IMeasure}
 */
@JsonIgnoreProperties({ "underlyingNames" })
@Builder
@Jacksonized
public class CustomMeasureForResource implements IMeasure, IHasUnderlyingMeasures {

	// This is a standard measure field
	@Getter
	@NonNull
	final String name;

	// This is a custom field
	final String customProperty;

	@JsonIgnore
	@Override
	public Set<String> getTags() {
		return Set.of("someCustomTag");
	}

	// It is useful to show underlying names, even if it may not be parsed back
	// (For documentation purposes)
	@JsonProperty(access = Access.READ_ONLY)
	@Override
	public List<String> getUnderlyingNames() {
		return List.of();
	}

	@Override
	public ITransformator wrapNode(IOperatorsFactory transformationFactory, CubeQueryStep adhocSubQuery) {
		return new EmptyTransformator();
	}
}
