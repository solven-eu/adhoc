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
package eu.solven.adhoc.query;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.ListMapEntryBasedTabularView.TabularEntry;
import eu.solven.adhoc.dataframe.tabular.ListMapEntryBasedTabularViewDrillThrough;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.lambda.LambdaEditor;
import eu.solven.adhoc.measure.lambda.LambdaEditor.ILambdaFilterEditor;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;

/**
 * End-to-end tests of the {@link StandardQueryOptions#DRILLTHROUGH} execution path. The cube engine emits one
 * {@link TabularEntry} per row returned by the table, with the merged groupBy as {@code coordinates} and the
 * per-aggregator (aliased) values as {@code values}.
 */
public class TestCubeQuery_Drillthrough extends ADagTest implements IAdhocTestConstants {

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("a", "a1", "k1", 123, "k2", 11));
		table().add(Map.of("a", "a2", "k1", 234, "k2", 22));
		table().add(Map.of("a", "a1", "k1", 345));
		table().add(Map.of("a", "a3", "k2", 44));
	}

	@Test
	public void testDrillthrough_groupByColumn_returnsOneRowPerSourceRow() {
		// Reproduces the Pivotable `simple` cube setup: many source rows (4 here, 16k there),
		// queried with a groupBy column and one Aggregator. DRILLTHROUGH must emit ONE entry per
		// source row (NOT one per groupBy slice) — i.e. duplicate `a=a1` coordinates must survive.
		forest.addMeasure(k1Sum);

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(k1Sum.getName())
				.groupByAlso("a")
				.option(StandardQueryOptions.DRILLTHROUGH)
				.build());

		ListMapEntryBasedTabularViewDrillThrough view = ListMapEntryBasedTabularViewDrillThrough.load(output);

		// 4 source rows (a1+k1, a2+k1, a1+k1, a3+k2-only) → 4 entries even though there are only
		// 3 distinct `a` coordinates. The two `a=a1` entries must NOT be collapsed to one.
		Assertions.assertThat(view.getEntries()).hasSize(4);
		long a1Count = view.getEntries().stream().filter(entry -> "a1".equals(entry.getCoordinates().get("a"))).count();
		Assertions.assertThat(a1Count).isEqualTo(2);
	}

	@Test
	public void testDrillthrough_singleMeasure_noGroupBy() {
		forest.addMeasure(k1Sum);

		ITabularView output = cube().execute(
				CubeQuery.builder().measure(k1Sum.getName()).option(StandardQueryOptions.DRILLTHROUGH).build());

		ListMapEntryBasedTabularViewDrillThrough view = ListMapEntryBasedTabularViewDrillThrough.load(output);

		// One entry per source row that carries a k1 value (in-memory table delivers raw rows).
		Assertions.assertThat(view.getEntries())
				.hasSize(4)
				.anySatisfy(entry -> Assertions.assertThat((Map) entry.getValues()).containsEntry("k1", 123))
				.anySatisfy(entry -> Assertions.assertThat((Map) entry.getValues()).containsEntry("k1", 234))
				.anySatisfy(entry -> Assertions.assertThat((Map) entry.getValues()).containsEntry("k1", 345))
				// Row a3 carries no k1 — the values map omits it (no SKIPPED_CELL needed since the schema is uniform).
				.anySatisfy(entry -> Assertions.assertThat((Map) entry.getValues()).doesNotContainKey("k1"));
	}

	@Test
	public void testDrillthrough_twoMeasures_groupedBy_singleRowPerSourceRow() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(k1Sum.getName())
				.measure(k2Sum.getName())
				.groupByAlso("a")
				.option(StandardQueryOptions.DRILLTHROUGH)
				.build());

		ListMapEntryBasedTabularViewDrillThrough view = ListMapEntryBasedTabularViewDrillThrough.load(output);

		// One entry per source row, each carrying both k1 and k2 columns where applicable.
		// This is the key contract: a single TabularEntry per DB row, not one per (row × measure).
		Assertions.assertThat(view.getEntries()).hasSize(4).allSatisfy(entry -> {
			Assertions.assertThat(entry.getCoordinates()).containsKey("a");
		})
				.anySatisfy(entry -> Assertions.assertThat((Map) entry.getValues())
						.containsEntry("k1", 123)
						.containsEntry("k2", 11))
				.anySatisfy(entry -> Assertions.assertThat((Map) entry.getValues())
						.containsEntry("k1", 234)
						.containsEntry("k2", 22))
				.anySatisfy(entry -> Assertions.assertThat((Map) entry.getValues())
						.containsEntry("k1", 345)
						.doesNotContainKey("k2"))
				.anySatisfy(entry -> Assertions.assertThat((Map) entry.getValues())
						.containsEntry("k2", 44)
						.doesNotContainKey("k1"));
	}

	@Test
	public void testShiftor_filteredColumnNotGroupedBy() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(Shiftor.builder()
				.name("shifted")
				.underlying(k1Sum.getName())
				.editorKey(LambdaEditor.class.getName())
				.editorOption(LambdaEditor.K_LAMBDA, (ILambdaFilterEditor) f -> SimpleFilterEditor.shift(f, "a", "a1"))
				.build());

		ITabularView output = cube()
				.execute(CubeQuery.builder().measure("shifted").option(StandardQueryOptions.DRILLTHROUGH).build());

		ListMapEntryBasedTabularViewDrillThrough view = ListMapEntryBasedTabularViewDrillThrough.load(output);

		// The Shiftor pins `a=a1`, so the merged row-inclusion filter (computed as `OR` over real aggregators of
		// `WHERE AND FILTER`) is `a=a1` — only the two `a=a1` source rows participate (a=a2 and a=a3 are not
		// matched by any real aggregator and therefore excluded from the DT output). `k2` is intentionally
		// absent (DT exposes only columns relevant to the query — here, the Shiftor's underlying `k1`). `a`
		// is auto-added to coordinates because it appears in the merged WHERE.
		Assertions.assertThat(view.getEntries()).hasSize(2).allSatisfy(entry -> {
			Assertions.assertThat(entry.getCoordinates()).isEqualTo(Map.of("a", "a1"));
			Assertions.assertThat(entry.getValues()).doesNotContainKey("k2");
		})
				.anySatisfy(entry -> Assertions.assertThat((Map) entry.getValues()).containsEntry("k1", 123))
				.anySatisfy(entry -> Assertions.assertThat((Map) entry.getValues()).containsEntry("k1", 345));
	}
}
