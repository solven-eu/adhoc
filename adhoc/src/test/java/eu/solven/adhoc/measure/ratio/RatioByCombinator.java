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
package eu.solven.adhoc.measure.ratio;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.measure.transformator.ICombinator;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.With;

/**
 * Demonstrate how to do a complex ratio through a single Combinator. This will consider the underlying measure, and do
 * the ratio after applying different filters to the numerator and denominator.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
public class RatioByCombinator implements ICombinator {
	@NonNull
	String name;

	@NonNull
	@Singular
	@With
	ImmutableSet<String> tags;

	@NonNull
	String underlying;

	@NonNull
	@Builder.Default
	IAdhocFilter numeratorFilter = IAdhocFilter.MATCH_ALL;

	@NonNull
	@Builder.Default
	IAdhocFilter denominatorFilter = IAdhocFilter.MATCH_ALL;

	/**
	 * @see eu.solven.adhoc.measure.combination.ICombination
	 */
	@NonNull
	@Builder.Default
	String combinationKey = SumCombination.KEY;

	/**
	 * @see eu.solven.adhoc.measure.combination.ICombination
	 */
	@NonNull
	@Builder.Default
	Map<String, ?> combinationOptions = Collections.emptyMap();

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return Collections.singletonList(underlying);
	}

	@Override
	public ITransformatorQueryStep wrapNode(AdhocFactories factories, CubeQueryStep step) {
		return new RatioByCombinatorQueryStep(this, factories, step);
	}
}
