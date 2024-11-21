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
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.solven.adhoc.aggregations.AdhocIdentity;
import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.IDecomposition;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.dag.AdhocQueryStep;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * This {@link IMeasure} will aggregate underlying measure into new coordinates. If multiple input coordinates write in
 * the same output coordinates, the values are aggregated with the configured aggregationKey.
 * 
 * A typical useCase is to generate a calculated column (e.g. a column which is the first letter of some underlying
 * column), or weigthed dispatching (e.g. input values with a column ranging any float between 0 and 1 should output a
 * column with either 0 or 1).
 * 
 * @author Benoit Lacelle
 * @see SingleBucketor
 */
@Value
@Builder
@Slf4j
public class Dispatchor implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;

	@Singular
	Set<String> tags;

	// A dispatcher has a single underlying measure, else it would be unclear how/when underlying measures should be
	// combined
	@NonNull
	String underlyingMeasure;

	/**
	 * @see IAggregation
	 */
	@NonNull
	@Default
	String aggregationKey = SumAggregator.KEY;

	/**
	 * @see IDecomposition
	 */
	@NonNull
	@Default
	String decompositionKey = AdhocIdentity.KEY;

	/**
	 * @see IDecomposition
	 */
	@NonNull
	@Default
	Map<String, ?> decompositionOptions = Collections.emptyMap();

	// // Accept a combinator key, to be applied on each groupBy
	// @NonNull
	// @Default
	// String combinatorKey = SumTransformation.KEY;
	//
	// @NonNull
	// @Default
	// Map<String, ?> combinatorOptions = Collections.emptyMap();

	// @NonNull
	// @Default
	// IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;

	@Override
	public List<String> getUnderlyingNames() {
		return Collections.singletonList(underlyingMeasure);
	}

	@Override
	public IHasUnderlyingQuerySteps wrapNode(IOperatorsFactory transformationFactory, AdhocQueryStep adhocSubQuery) {
		return new DispatchorQueryStep(this, transformationFactory, adhocSubQuery);
	}

}
