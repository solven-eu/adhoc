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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.measure.lambda.LambdaCombination;
import eu.solven.adhoc.measure.lambda.LambdaCombination.ILambdaCombination;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.measure.transformator.ICombinator;
import eu.solven.adhoc.measure.transformator.IHasCombinationKey;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.cube.IHasGroupBy;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.With;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Combinator} is a {@link IMeasure} which combines the underlying measures for current coordinate.
 * 
 * @author Benoit Lacelle
 */
// TODO toString should be explicit, and skip `combinationOptions` if empty
@Value
@Builder(toBuilder = true)
@Jacksonized
@Slf4j
public class Combinator implements ICombinator, IHasCombinationKey {
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

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return underlyings;
	}

	/**
	 *
	 * @param hasUnderlyings
	 *            the parent IHasUnderlyingMeasures generating a Combinator needed these options
	 * @param explicitOptions
	 *            some options provided explicitly
	 * @return
	 */
	public static Map<String, ?> makeAllOptions(IHasUnderlyingMeasures hasUnderlyings, Map<String, ?> explicitOptions) {
		Map<String, Object> allOptions = new LinkedHashMap<>();

		// Default options
		// Enable visibility to the Combinator of the name of the received values
		allOptions.put(KEY_UNDERLYING_NAMES, hasUnderlyings.getUnderlyingNames());

		// TODO Should we provide the Set of columns guaranteed to be available in the slice?
		// It seemed simpler to provide the whole measure for now
		if (hasUnderlyings instanceof IHasGroupBy hasGroupBy) {
			IGroupBy groupBy = hasGroupBy.getGroupBy();
			allOptions.put(KEY_GROUPBY_COLUMNS, groupBy.getSortedColumns());
		}

		allOptions.put(KEY_MEASURE, hasUnderlyings);

		// override with explicit options
		allOptions.putAll(explicitOptions);

		return allOptions;
	}

	public static IMeasure sum(String first, String second) {
		return Combinator.builder()
				.name("sum(%s,%s)".formatted(first, second))
				.combinationKey(SumCombination.KEY)
				.underlying(first)
				.underlying(second)
				.build();
	}

	/**
	 * Lombok @Builder
	 */
	public static class CombinatorBuilder {
		public CombinatorBuilder lambda(ILambdaCombination lambda) {
			return combinationKey(LambdaCombination.class.getName()).combinationOption(LambdaCombination.K_LAMBDA,
					lambda);
		}
	}
}
