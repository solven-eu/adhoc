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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.measure.transformator.ICombineUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.IHasAggregationKey;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import eu.solven.adhoc.measure.transformator.step.PartitionorQueryStep;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
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
 * This {@link IMeasure} will aggregate underlying measure, evaluated at buckets defined by a {@link IAdhocGroupBy}, and
 * aggregated through an {@link IAggregation}.
 *
 * A typical use-case is the Foreign-Exchange conversion, as we need to evaluate underlying measures on a per-currency
 * basis.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder(toBuilder = true)
@Slf4j
@Jacksonized
public class Partitionor implements IMeasure, ICombineUnderlyingMeasures, IHasAggregationKey, IHasGroupBy {
	@NonNull
	String name;

	@NonNull
	@Singular
	@With
	ImmutableSet<String> tags;

	@NonNull
	@Singular
	ImmutableList<String> underlyings;

	@NonNull
	@Default
	String aggregationKey = SumAggregation.KEY;

	@Singular
	ImmutableMap<String, ?> aggregationOptions;

	// Accept a combinator key, to be applied on each groupBy
	@NonNull
	@Default
	String combinationKey = SumCombination.KEY;

	@NonNull
	@Singular
	ImmutableMap<String, ?> combinationOptions;

	@NonNull
	@Default
	IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return getUnderlyings();
	}

	@Override
	public ITransformatorQueryStep wrapNode(AdhocFactories factories, CubeQueryStep queryStep) {
		return new PartitionorQueryStep(this, factories, queryStep);
	}

}
