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
package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.aggregations.sum.SumCombination;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.dag.AdhocQueryStep;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * This {@link IMeasure} will aggregate underlying measure, evaluated at buckets defined by a {@link IAdhocGroupBy}, and
 * aggregated through an {@link IAggregation}.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@Slf4j
public class Bucketor implements IMeasure, IHasUnderlyingMeasures, IHasCombinationKey {
	@NonNull
	String name;

	@Singular
	Set<String> tags;

	@NonNull
	@Singular
	List<String> underlyings;

	@NonNull
	@Default
	String aggregationKey = SumAggregator.KEY;

	// Accept a combinator key, to be applied on each groupBy
	@NonNull
	@Default
	String combinationKey = SumCombination.KEY;

	@NonNull
	@Default
	Map<String, ?> combinationOptions = Collections.emptyMap();

	@NonNull
	@Default
	IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return getUnderlyings();
	}

	@Override
	public Map<String, ?> getCombinationOptions() {
		return makeAllOptions(this, combinationOptions);
	}

	public static Map<String, ?> makeAllOptions(IHasUnderlyingMeasures hasUnderlyings, Map<String, ?> explicitOptions) {
		Map<String, Object> allOptions = new HashMap<>();

		// Default options
		allOptions.put("underlyingNames", hasUnderlyings.getUnderlyingNames());

		// override with explicit options
		allOptions.putAll(explicitOptions);

		return allOptions;
	}

	@Override
	public IHasUnderlyingQuerySteps wrapNode(IOperatorsFactory transformationFactory, AdhocQueryStep queryStep) {
		return new BucketorQueryStep(this, transformationFactory, queryStep);
	}

}
