/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure.routing;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.measure.transformator.ICombinator;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.With;

/**
 * Routes a query to one of several underlying measures based on a caller-provided {@link Function} of the current
 * {@link CubeQueryStep}. Useful in migration scenarios (typically Atoti → Adhoc) where a logical measure is computed
 * from different physical measures depending on the slice of data the query touches — e.g. one measure for data before
 * a modelling cutoff, another for data after.
 *
 * <p>
 * The {@link #routeFunction} is invoked once per {@link CubeQueryStep}, and must return a measure name present in
 * {@link #underlyings}. The closed list is required so the engine can plan dependencies and surface a clear error if
 * the function returns an unknown name at runtime.
 *
 * <p>
 * <b>Cross-boundary queries are not supported.</b> The function returns exactly one underlying per step, which means
 * queries whose filter spans both sides of the routing boundary <i>without</i> the routing column being part of the
 * groupBy will be silently routed to whichever branch the function chose — losing the contributions of the other side.
 * Callers must ensure their queries do not straddle the boundary in this way; the framework does not detect it because
 * the function is opaque. A future iteration can replace the function signature with a decomposition strategy when
 * cross-boundary support becomes necessary.
 *
 * <p>
 * <b>JSON / declarative resource loading is not supported.</b> A {@link Function} cannot be Jackson-serialized, so a
 * {@code RoutingMeasure} cannot be declared in a measure-forest YAML/JSON resource file (see
 * {@code MeasureForestFromResource}). Instances must be registered programmatically. If declarative configuration is
 * required, use the standard {@link eu.solven.adhoc.model.measure.Combinator} family — they identify their logic by a
 * string {@code combinationKey} resolved through {@code IOperatorFactory}, which round-trips cleanly.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
public class RoutingMeasure implements ICombinator {
	@NonNull
	String name;

	@NonNull
	@Singular
	@With
	ImmutableSet<String> tags;

	/**
	 * Closed set of underlying measure names {@link #routeFunction} may return. Listed eagerly so the engine can plan
	 * dependencies and so an unknown return value at runtime can be reported with the full universe in the error
	 * message.
	 */
	@NonNull
	@Singular
	ImmutableList<String> underlyings;

	/**
	 * Picks the underlying measure to query for the given {@link CubeQueryStep}. Must return a value present in
	 * {@link #underlyings}. The function should be cheap and pure: it is invoked once per step and the engine treats
	 * its result as a deterministic property of the step.
	 */
	@JsonIgnore
	@NonNull
	Function<CubeQueryStep, String> routeFunction;

	/**
	 * Carried for {@link ICombinator} compliance but not actually invoked: {@link RoutingMeasureQueryStep} performs a
	 * direct passthrough of the chosen underlying's values, without engaging the combination machinery. Defaults to
	 * {@link SumCombination#KEY} — a harmless choice given a single underlying — but the value is irrelevant.
	 */
	@NonNull
	@Builder.Default
	String combinationKey = SumCombination.KEY;

	/**
	 * @see #getCombinationKey()
	 */
	@NonNull
	@Builder.Default
	Map<String, ?> combinationOptions = Collections.emptyMap();

	@JsonIgnore
	@Override
	public List<String> getUnderlyingNames() {
		return underlyings;
	}

	@Override
	public String queryStepClass() {
		return RoutingMeasureQueryStep.class.getName();
	}
}
