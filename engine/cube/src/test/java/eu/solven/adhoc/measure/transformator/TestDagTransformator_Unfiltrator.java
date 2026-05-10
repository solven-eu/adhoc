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
package eu.solven.adhoc.measure.transformator;

import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ATestDagInMemory;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.ListMapEntryBasedTabularViewDrillThrough;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.model.measure.Unfiltrator;
import eu.solven.adhoc.options.StandardQueryOptions;

/**
 * End-to-end coverage of {@link Unfiltrator} executed through
 * {@link eu.solven.adhoc.measure.transformator.step.UnfiltratorQueryStep}. The contract under test: an
 * {@link Unfiltrator} widens the query filter for its underlying measure, neutralising the constraints on the declared
 * columns (Suppress) or keeping only those constraints (Retain). Useful for hierarchical share-of-total-style measures.
 */
public class TestDagTransformator_Unfiltrator extends ATestDagInMemory implements IAdhocTestConstants {

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("a", "a1", "k1", 100));
		table().add(Map.of("a", "a1", "k1", 50));
		table().add(Map.of("a", "a2", "k1", 200));
	}

	// Suppress mode: the unfiltrator removes the filter on `a`, so even though the query filters `a=a1`, the
	// underlying k1 sees the whole table.
	@Test
	public void testUnfiltrator_suppressMode_widensFilter() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(
				Unfiltrator.builder().name("k1_unfilteredOnA").underlying(k1Sum.getName()).column("a").build());

		CubeQuery query = CubeQuery.builder().measure("k1_unfilteredOnA", "k1").andFilter("a", "a1").build();

		ITabularView output = cube().execute(query);
		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		// k1 stays filtered (sum over a=a1 only), k1_unfilteredOnA sees the whole table.
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("k1", 0L + 100 + 50, "k1_unfilteredOnA", 0L + 100 + 50 + 200));
	}

	// Retain mode: only the listed columns keep their filter; everything else is widened. The fluent helper
	// `unfilterOthersThan(...)` is the canonical entry point for this mode.
	@Test
	public void testUnfiltrator_retainMode_keepsOnlyListedColumns() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(
				Unfiltrator.builder().name("k1_retainA").underlying(k1Sum.getName()).unfilterOthersThan("a").build());

		// Filter on both `a` and a different column — only the `a` filter survives in the underlying step.
		CubeQuery query = CubeQuery.builder()
				.measure("k1_retainA")
				.andFilter("a", "a1")
				.andFilter(ColumnFilter.matchEq("a", "a1"))
				.build();

		ITabularView output = cube().execute(query);
		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("k1_retainA", 0L + 100 + 50));
	}

	// matchAll filter case: the unfiltrator must not synthesise an unexpected filter when there is nothing to widen.
	@Test
	public void testUnfiltrator_matchAllFilter_returnsFullAggregate() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(
				Unfiltrator.builder().name("k1_unfilteredOnA").underlying(k1Sum.getName()).column("a").build());

		ITabularView output = cube().execute(CubeQuery.builder().measure("k1_unfilteredOnA").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("k1_unfilteredOnA", 0L + 100 + 50 + 200));
	}

	// DRILLTHROUGH over an Unfiltrator: the merged WHERE for row inclusion is the OR of every aggregator's
	// (WHERE AND FILTER). When the only queried measure is an Unfiltrator that suppresses the filter on `a`, the
	// underlying step's filter is matchAll, so the merged WHERE collapses to matchAll and EVERY source row appears
	// in the DT output — including rows that the cube-side filter `a=a1` would have excluded.
	@Test
	public void testUnfiltrator_drillthrough_suppressMode_emitsEverySourceRow() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(
				Unfiltrator.builder().name("k1_unfilteredOnA").underlying(k1Sum.getName()).column("a").build());

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure("k1_unfilteredOnA")
				.andFilter("a", "a1")
				.option(StandardQueryOptions.DRILLTHROUGH)
				.build());

		ListMapEntryBasedTabularViewDrillThrough view = ListMapEntryBasedTabularViewDrillThrough.load(output);

		// All 3 source rows participate (a1+100, a1+50, a2+200) — the cube-side `a=a1` filter is widened away.
		// The merged WHERE for row inclusion is computed from the underlying TableQueryV4 (which has matchAll
		// filter for the Unfiltrator's underlying), NOT from the cube-side filter — so `a` is NOT added to the
		// DT coordinates. We assert on the values instead.
		Assertions.assertThat(view.getEntries()).hasSize(3);
		long sumK1 = view.getEntries().stream().mapToLong(e -> ((Number) e.getValues().get("k1")).longValue()).sum();
		Assertions.assertThat(sumK1)
				.as("every k1 value must reach DT including the a=a2 row")
				.isEqualTo(100 + 50 + 200);
	}

	// DRILLTHROUGH over a Retain-mode Unfiltrator: the column listed in `unfilterOthersThan` keeps its filter;
	// any other filtered column is widened. Here we filter `a=a1` AND `b=b1`, retain only `a`. The underlying
	// step's filter is therefore `a=a1`, and the merged WHERE collapses to that — so DT emits only the `a=a1`
	// rows even though `b=b1` was also requested.
	@Test
	public void testUnfiltrator_drillthrough_retainMode_keepsOnlyListedColumnFilter() {
		// Add a `b` column on the existing rows so the retain-mode test has something to widen away.
		table().add(Map.of("a", "a1", "b", "b1", "k1", 7));
		table().add(Map.of("a", "a1", "b", "b2", "k1", 8));
		table().add(Map.of("a", "a2", "b", "b1", "k1", 9));

		forest.addMeasure(k1Sum);
		forest.addMeasure(
				Unfiltrator.builder().name("k1_retainA").underlying(k1Sum.getName()).unfilterOthersThan("a").build());

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure("k1_retainA")
				.andFilter("a", "a1")
				.andFilter("b", "b1")
				.option(StandardQueryOptions.DRILLTHROUGH)
				.build());

		ListMapEntryBasedTabularViewDrillThrough view = ListMapEntryBasedTabularViewDrillThrough.load(output);

		// Underlying step's filter is `a=a1` (b filter was widened). Both base rows with a=a1 appear, plus the two
		// new a=a1 rows (b=b1 and b=b2). The single a=a2 row is excluded.
		Assertions.assertThat(view.getEntries()).hasSize(4).allSatisfy(e -> {
			Assertions.assertThat((Map) e.getCoordinates()).containsEntry("a", "a1");
		});
	}

	// Mixed query: a filtered measure (k1) AND an Unfiltrator (k1_unfilteredOnA) at the same time. The merged
	// WHERE = (k1's filter `a=a1`) OR (Unfiltrator's filter `matchAll`) = matchAll. So every source row appears.
	@Test
	public void testUnfiltrator_drillthrough_mixedWithFilteredMeasure() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(
				Unfiltrator.builder().name("k1_unfilteredOnA").underlying(k1Sum.getName()).column("a").build());

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure("k1", "k1_unfilteredOnA")
				.andFilter("a", "a1")
				.option(StandardQueryOptions.DRILLTHROUGH)
				.build());

		ListMapEntryBasedTabularViewDrillThrough view = ListMapEntryBasedTabularViewDrillThrough.load(output);

		Assertions.assertThat(view.getEntries()).hasSize(3);
	}

}
