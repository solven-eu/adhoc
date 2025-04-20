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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

import eu.solven.adhoc.dag.step.AdhocQueryStep;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.transformator.FiltratorQueryStep;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.ITransformator;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * A Filtrator is an {@link IMeasure} which is filtering another {@link IMeasure} given a {@link IAdhocFilter}. The
 * input {@link IAdhocFilter} will be `AND`-ed with {@link AdhocQueryStep} own {@link IAdhocFilter}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
@Slf4j
public class Filtrator implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;

	@Singular
	Set<String> tags;

	@NonNull
	String underlying;

	@NonNull
	IAdhocFilter filter;

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return Collections.singletonList(underlying);
	}

	@Override
	public ITransformator wrapNode(IOperatorsFactory transformationFactory, AdhocQueryStep step) {
		return new FiltratorQueryStep(this, transformationFactory, step);
	}

}
