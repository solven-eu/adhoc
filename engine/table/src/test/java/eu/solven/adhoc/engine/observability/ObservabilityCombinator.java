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
package eu.solven.adhoc.engine.observability;

import java.util.List;

import org.jspecify.annotations.NonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.measure.transformator.ICombinator;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.IMeasure;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Singular;
import lombok.Value;
import lombok.With;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Combinator} is a {@link IMeasure} which combines the underlying measures for current coordinate.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@Slf4j
public class ObservabilityCombinator implements ICombinator {
	@NonNull
	String name;

	@NonNull
	@Singular
	@With
	ImmutableSet<String> tags;

	@NonNull
	@Singular
	ImmutableList<String> underlyings;

	/**
	 * @see eu.solven.adhoc.measure.combination.ICombination
	 */
	@NonNull
	@Default
	String combinationKey = SumCombination.KEY;

	/**
	 * @see eu.solven.adhoc.measure.combination.ICombination
	 */
	@NonNull
	@Singular
	ImmutableMap<String, ?> combinationOptions;

	@Override
	public List<String> getUnderlyingNames() {
		return underlyings;
	}

	@Override
	public String queryStepClass() {
		return ObservabilityTestQueryStep.class.getName();
	}

	public static IMeasure sum(String first, String second) {
		return ObservabilityCombinator.builder()
				.name("observability(%s,%s)".formatted(first, second))
				.combinationKey(SumCombination.KEY)
				.underlying(first)
				.underlying(second)
				.build();
	}
}
