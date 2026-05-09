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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.model.measure.IMeasure;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Singular;
import lombok.Value;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

/**
 * Decomposes a {@link CubeQueryStep} into one or more underlying steps via a caller-provided {@link IRoutingLogic},
 * then coalesces their per-slice values. Useful in migration scenarios (typically Atoti → Adhoc) where a logical
 * measure is computed from different physical measures or filters depending on the slice — e.g. legacy table for data
 * before a modelling cutoff, modern table afterwards.
 *
 * <p>
 * Two operating modes share the same machinery:
 *
 * <ul>
 * <li><b>Single-step (passthrough)</b>: the routing logic returns one {@link CubeQueryStep}. The output is that step's
 * value, slice by slice.</li>
 * <li><b>Multi-step (decomposition)</b>: the routing logic returns several {@link CubeQueryStep} instances, typically
 * with disjoint filters that together partition the parent step's filter — e.g. {@code (date < cutoff)} and
 * {@code (date >= cutoff)}, each potentially targeting a different underlying measure. Per-slice results are coalesced
 * (first-non-null wins): when filters are disjoint, exactly one sub-step produces a value for any given slice, so
 * coalesce is a structural pick rather than an aggregation.</li>
 * </ul>
 *
 * <p>
 * <b>Why coalesce, not SUM:</b> the contract is "one slice ↔ one sub-step". SUM would also be correct in that contract
 * (a single non-null value sums to itself), but it would silently double-count if filters happen to overlap. Coalesce
 * surfaces overlap as a "first wins" anomaly that's easier to spot than a doubled number; routing is not an aggregation
 * operation.
 *
 * <p>
 * <b>Disjoint-filter responsibility lies with the caller.</b> The framework cannot detect overlap because the routing
 * logic is opaque. With overlapping filters, results become order-dependent on whichever sub-step the engine reaches
 * first.
 *
 * <p>
 * <b>Linearity matters when combining across boundaries.</b> Coalescing per-side aggregations is correct when the
 * routing column is in the groupBy (each output slice is fed by exactly one sub-step) <i>or</i> when the user's filter
 * already restricts the query to one side. With non-linear aggregators (MAX, RANK, percentile) on a query that crosses
 * the boundary without the routing column in the groupBy, the result would be wrong even with SUM; refuse such queries
 * inside the routing logic rather than dispatching them.
 *
 * <p>
 * <b>JSON round-trip is partial.</b> The class is {@code @Jacksonized}, so {@link #name}, {@link #tags},
 * {@link #underlyings} and {@link #routingOptions} serialize and deserialize cleanly — sufficient for documentation
 * tools (forest summaries, dependency-graph generators) that need to introspect the measure's declared dependencies.
 * The {@link #routingLogic} reference cannot be serialized and is excluded from JSON; a deserialized instance has
 * {@code routingLogic == null} and is therefore <i>spec-only</i> — usable for introspection but not for query
 * execution. The {@link #routingOptions} field is a placeholder for a future declarative path: an extension to
 * {@code IOperatorFactory} (mirroring how {@link eu.solven.adhoc.measure.combination.ICombination} and
 * {@link eu.solven.adhoc.measure.decomposition.IDecomposition} are instantiated from a string key plus options map)
 * would let an {@link IRoutingLogic} implementation be looked up by FQCN at runtime, with {@link #routingOptions} fed
 * to its {@code Map<String, ?>}-arg constructor.
 *
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
// `routingLogic` cannot be JSON-serialized, so a deserialized RoutingMeasure has it null.
// Excluding it from equality keeps `Jackson roundtrip → equals(original)` true for the spec-only use case.
@EqualsAndHashCode(exclude = "routingLogic")
public class RoutingMeasure implements IMeasure, IHasUnderlyingMeasures {
	// `@NonNull` is required for runtime enforcement at builder.build(): Lombok does not consult package-level
	// `@NullMarked` (JSpecify) to decide where to insert null checks — see
	// https://github.com/projectlombok/lombok/issues/3861 and the pinning test
	// `TestRoutingMeasure#testLombok_doesNotPickUpPackageNullMarked_buildsWithNullField`.
	@NonNull
	String name;

	@NonNull
	@Singular
	@With
	ImmutableSet<String> tags;

	/**
	 * Covering set of underlying measure names that {@link #routingLogic} may reference. Required for utilities that
	 * introspect a {@link eu.solven.adhoc.measure.forest.MeasureForest} statically — dependency-graph generators,
	 * forest summaries, the explain output — none of which can run the routing logic to discover its targets.
	 *
	 * <p>
	 * Also enforced at query time: every {@link CubeQueryStep} returned by {@link #routingLogic} must reference a
	 * measure whose name is in this set, otherwise {@link RoutingMeasureQueryStep} throws. This makes the field
	 * load-bearing rather than informational, which keeps documentation in sync with runtime behaviour.
	 */
	@Singular
	ImmutableList<String> underlyings;

	/**
	 * The decomposition strategy. Held as a direct reference rather than resolved through a string key, so a lambda is
	 * enough at the call site. Excluded from JSON via {@code @JsonIgnore}; a deserialized instance has this null and is
	 * therefore spec-only.
	 */
	@JsonIgnore
	@Nullable
	IRoutingLogic routingLogic;

	/**
	 * Reserved for the future declarative path: when an {@code IOperatorFactory} method exists to resolve an
	 * {@link IRoutingLogic} implementation from a class FQCN plus an options map, this map will be passed to the
	 * resolved implementation's {@code Map<String, ?>}-arg constructor. Currently unused by
	 * {@link RoutingMeasureQueryStep} (the configured {@link #routingLogic} is invoked directly), but populating it
	 * documents intent and keeps the API stable.
	 */
	@Builder.Default
	Map<String, ?> routingOptions = Collections.emptyMap();

	/**
	 * Returns the {@link #underlyings} covering set. The engine uses it to plan the DAG; tooling uses it to render
	 * dependency graphs and forest documentation. {@link #routingLogic} is constrained to return steps whose measure is
	 * in this set.
	 */
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
