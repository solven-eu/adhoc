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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ATestDagInMemory;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.value.ComparingMatcher;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.model.measure.Aggregator;

/**
 * DAG-level tests for {@link RoutingMeasure} on a date-cutover migration scenario: the underlying data model changed on
 * {@code 2026-01-01}, so a single logical measure {@code dRouted} must route to a "legacy" or a "modern" sub-query
 * depending on which side of the cutoff a slice falls. The {@link IRoutingLogic} returns a list of
 * {@link CubeQueryStep}, one per side, each with the appropriate date filter ANDed onto the parent step's filter. Their
 * per-slice values are coalesced ({@link RoutingMeasure} hardcodes coalesce as the cross-step combine), which works
 * because the per-side filters are disjoint by construction here.
 */
public class TestDag_RoutingMeasure extends ATestDagInMemory {

	// Modelling cutoff: rows with date < CUTOFF use the legacy schema, rows with date >= CUTOFF use the modern schema.
	// Strings compared lexicographically work for ISO-8601 dates.
	static final String CUTOFF = "2026-01-01";

	@Override
	@BeforeEach
	public void feedTable() {
		// Distinct columns per side: legacy rows carry `d_legacy`, modern rows carry `d_modern`.
		// This mirrors a real Atoti-style migration where the underlying data model changed at the cutover —
		// not just the filter. A bug that routes to the wrong measure (or skips the date filter) now produces
		// a wrong number instead of accidentally returning the right one because both measures aggregate the
		// same column.
		table().add(Map.of("date", "2025-11-15", "country", "FR", "d_legacy", 100));
		table().add(Map.of("date", "2025-12-31", "country", "US", "d_legacy", 200));
		table().add(Map.of("date", "2026-01-15", "country", "FR", "d_modern", 300));
		table().add(Map.of("date", "2026-03-20", "country", "US", "d_modern", 400));
	}

	@BeforeEach
	public void registerBaseMeasures() {
		// Each aggregator reads its own column — by design legacy and modern do not overlap, so a misrouted
		// query gets a 0 / null instead of the silently-correct value that a shared column would give.
		forest.addMeasure(Aggregator.builder()
				.name("d_legacy")
				.columnName("d_legacy")
				.aggregationKey(SumAggregation.KEY)
				.build());
		forest.addMeasure(Aggregator.builder()
				.name("d_modern")
				.columnName("d_modern")
				.aggregationKey(SumAggregation.KEY)
				.build());
	}

	/**
	 * @return a routing logic that splits any step into a legacy and a modern sub-step around {@link #CUTOFF}.
	 */
	private static IRoutingLogic dateCutoffRouter() {
		ISliceFilter beforeFilter =
				ColumnFilter.builder().column("date").matching(ComparingMatcher.strictlyLowerThan(CUTOFF)).build();
		ISliceFilter afterFilter =
				ColumnFilter.builder().column("date").matching(ComparingMatcher.greaterThanOrEqual(CUTOFF)).build();

		return step -> {
			CubeQueryStep legacy = CubeQueryStep.edit(step)
					.measure("d_legacy")
					.filter(FilterBuilder.and(step.getFilter(), beforeFilter).optimize())
					.build();
			CubeQueryStep modern = CubeQueryStep.edit(step)
					.measure("d_modern")
					.filter(FilterBuilder.and(step.getFilter(), afterFilter).optimize())
					.build();
			return List.of(legacy, modern);
		};
	}

	@Test
	public void testGrandTotal_coalescesBothSidesAcrossCutoff() {
		// Plain date-cutoff routing: every query is decomposed into a legacy side + a modern side, coalesced.
		// With the routing column (`date`) NOT in the groupBy, each output slice is grandTotal — and gets one
		// non-null contribution from each side, which coalesce can't merge structurally. Combined with the
		// underlying SUM aggregator, the result is one of the two sums (whichever sub-step lands first).
		// This is the "non-linear-without-groupBy" case the doc warns about; it's exercised here purely to pin
		// today's behaviour, not to claim it's the right answer for the user.
		forest.addMeasure(RoutingMeasure.builder()
				.name("dRouted")
				.underlying("d_legacy")
				.underlying("d_modern")
				.routingLogic(dateCutoffRouter())
				.build());

		ITabularView output = cube().execute(CubeQuery.builder().measure("dRouted").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);
		// One of: 0L+100+200 (legacy total) or 0L+300+400 (modern total). Coalesce picks the first non-null.
		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1);
		Object dRouted = mapBased.getCoordinatesToValues().get(Collections.emptyMap()).get("dRouted");
		Assertions.assertThat(dRouted).isIn(0L + 100 + 200, 0L + 300 + 400);
	}

	@Test
	public void testGroupByDate_oneSidePerOutputSlice() {
		// Adding `date` to the groupBy means each output slice corresponds to exactly one date — and therefore
		// to exactly one side of the cutoff. Coalesce becomes a structural pick: the side that doesn't apply
		// returns null, the other returns the value, and coalesce passes it through cleanly.
		forest.addMeasure(RoutingMeasure.builder()
				.name("dRouted")
				.underlying("d_legacy")
				.underlying("d_modern")
				.routingLogic(dateCutoffRouter())
				.build());

		ITabularView output = cube().execute(CubeQuery.builder().measure("dRouted").groupByAlso("date").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(4)
				.containsEntry(Map.of("date", "2025-11-15"), Map.of("dRouted", 0L + 100))
				.containsEntry(Map.of("date", "2025-12-31"), Map.of("dRouted", 0L + 200))
				.containsEntry(Map.of("date", "2026-01-15"), Map.of("dRouted", 0L + 300))
				.containsEntry(Map.of("date", "2026-03-20"), Map.of("dRouted", 0L + 400));
	}

	@Test
	public void testFilterDateEntirelyBeforeCutoff_modernSideContributesNothing() {
		// User filter restricts to date < CUTOFF. The modern sub-step's filter becomes
		// `(date < CUTOFF) AND (date >= CUTOFF)` which optimizes to MATCH_NONE; the engine still queries it but
		// it returns no rows. Coalesce of (legacyValue, null) returns legacyValue.
		forest.addMeasure(RoutingMeasure.builder()
				.name("dRouted")
				.underlying("d_legacy")
				.underlying("d_modern")
				.routingLogic(dateCutoffRouter())
				.build());

		ISliceFilter userFilter =
				ColumnFilter.builder().column("date").matching(ComparingMatcher.strictlyLowerThan(CUTOFF)).build();

		ITabularView output = cube().execute(CubeQuery.builder().measure("dRouted").andFilter(userFilter).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("dRouted", 0L + 100 + 200));
	}

	@Test
	public void testHeterogeneousRouting_perSidePerCountry() {
		// The scenario the user asked for: each side of the cutoff applies a DIFFERENT country filter.
		// "Before the cutoff, only count FR; after the cutoff, only count US." A single-step routing API
		// could not express this — only the multi-step API can, because the two sub-queries have unrelated
		// shapes (different filters, potentially different measures).
		ISliceFilter beforeFilter = FilterBuilder
				.and(ColumnFilter.builder().column("date").matching(ComparingMatcher.strictlyLowerThan(CUTOFF)).build(),
						ColumnFilter.matchEq("country", "FR"))
				.optimize();
		ISliceFilter afterFilter = FilterBuilder.and(
				ColumnFilter.builder().column("date").matching(ComparingMatcher.greaterThanOrEqual(CUTOFF)).build(),
				ColumnFilter.matchEq("country", "US")).optimize();

		forest.addMeasure(RoutingMeasure.builder()
				.name("dRouted")
				.underlying("d_legacy")
				.underlying("d_modern")
				.routingLogic(step -> {
					CubeQueryStep legacy = CubeQueryStep.edit(step)
							.measure("d_legacy")
							.filter(FilterBuilder.and(step.getFilter(), beforeFilter).optimize())
							.build();
					CubeQueryStep modern = CubeQueryStep.edit(step)
							.measure("d_modern")
							.filter(FilterBuilder.and(step.getFilter(), afterFilter).optimize())
							.build();
					return List.of(legacy, modern);
				})
				.build());

		// With `date` in groupBy, each output slice corresponds to exactly one row of the fixture and routing is
		// unambiguous: FR-before-cutoff (100) and US-after-cutoff (400) survive; the others are filtered out.
		ITabularView output = cube().execute(CubeQuery.builder().measure("dRouted").groupByAlso("date").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("date", "2025-11-15"), Map.of("dRouted", 0L + 100))
				.containsEntry(Map.of("date", "2026-03-20"), Map.of("dRouted", 0L + 400));
	}

	@Test
	public void testEmptyRoute_yieldsEmptyOutput() {
		// An empty route is a legitimate "this measure has nothing to contribute for this step" signal —
		// e.g. the routing logic determined no underlying applies. The output is an empty cuboid, not an exception.
		forest.addMeasure(RoutingMeasure.builder()
				.name("dRouted")
				.underlying("d_legacy")
				.routingLogic(step -> Collections.emptyList())
				.build());

		ITabularView output = cube().execute(CubeQuery.builder().measure("dRouted").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);
		Assertions.assertThat(mapBased.getCoordinatesToValues()).isEmpty();
	}

	@Test
	public void testReturnsUnknownMeasure_throws() {
		forest.addMeasure(RoutingMeasure.builder()
				.name("dRouted")
				.underlying("d_legacy")
				.routingLogic(step -> List.of(CubeQueryStep.edit(step).measure("d_modern").build()))
				.build());

		// `d_modern` is not in this RoutingMeasure's `underlyings` covering set, so the QueryStep refuses it
		// even though `d_modern` exists in the forest.
		Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder().measure("dRouted").build()))
				.hasStackTraceContaining("dRouted")
				.hasStackTraceContaining("d_modern")
				.hasStackTraceContaining("[d_legacy]");
	}
}
