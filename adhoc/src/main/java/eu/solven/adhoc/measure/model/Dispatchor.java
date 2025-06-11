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
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.column.generated_column.IColumnGenerator;
import eu.solven.adhoc.column.generated_column.IMayHaveColumnGenerator;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.combination.AdhocIdentity;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.transformator.IHasAggregationKey;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.measure.transformator.step.DispatchorQueryStep;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.With;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * This {@link IMeasure} will aggregate underlying measure into new coordinates. If multiple input coordinates write in
 * the same output coordinates, the values are aggregated with the configured aggregationKey.
 * <p>
 * A typical useCase is to generate a calculated column (e.g. a column which is the first letter of some underlying
 * column), or weigthed dispatching (e.g. input values with a column ranging any float between 0 and 1 should output a
 * column with either 0 or 1).
 *
 * @author Benoit Lacelle
 */
@Value
@Builder(toBuilder = true)
@Slf4j
@Jacksonized
public class Dispatchor implements IMeasure, IHasUnderlyingMeasures, IHasAggregationKey, IMayHaveColumnGenerator {
	@NonNull
	String name;

	@NonNull
	@Singular
	@With
	ImmutableSet<String> tags;

	// Developer note: if you wish having multiple underlings: either you add a Combinator an underlying to this
	// Dispatcher. Or make a new Dispatchor implementation. But do not try (for now, until you know it will work) making
	// a multi-underlyings Dispatchor. It would be complex for Users, and very complex to implement with current
	// AdhocEngine (e.g. as the adhocQuerySteps would be expanded by decomposition and by underlyings).
	@NonNull
	String underlying;

	/**
	 * The aggregation used when multiple input contributes into the same output coordinates.
	 *
	 * @see IAggregation
	 */
	@NonNull
	@Default
	String aggregationKey = SumAggregation.KEY;

	@Singular
	ImmutableMap<String, ?> aggregationOptions;

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
	@Singular
	ImmutableMap<String, ?> decompositionOptions;

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return Collections.singletonList(underlying);
	}

	@Override
	public ITransformatorQueryStep wrapNode(AdhocFactories factories, CubeQueryStep adhocSubQuery) {
		return new DispatchorQueryStep(this, factories, adhocSubQuery);
	}

	@Override
	public Optional<IColumnGenerator> optColumnGenerator(IOperatorFactory operatorFactory) {
		IDecomposition decomposition =
				operatorFactory.makeDecomposition(getDecompositionKey(), getDecompositionOptions());

		if (decomposition instanceof IColumnGenerator columnGenerator) {
			return Optional.of(columnGenerator);
		} else {
			return Optional.empty();
		}
	}

}
