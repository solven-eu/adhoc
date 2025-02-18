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
package eu.solven.adhoc.measure.step;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.measure.IMeasure;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IHasGroupBy;
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
 * A typical use-case is the Foreign-Exchange conversion, as we need to evaluate underlying measures on a per-currency
 * basis.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@Slf4j
public class Bucketor implements IMeasure, ICombineUnderlyingMeasures, IHasGroupBy {
	@NonNull
	String name;

	@Singular
	Set<String> tags;

	@NonNull
	@Singular
	List<String> underlyings;

	@NonNull
	@Default
	String aggregationKey = SumAggregation.KEY;

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
	public ITransformator wrapNode(IOperatorsFactory transformationFactory, AdhocQueryStep queryStep) {
		return new BucketorQueryStep(this, transformationFactory, queryStep);
	}

}
