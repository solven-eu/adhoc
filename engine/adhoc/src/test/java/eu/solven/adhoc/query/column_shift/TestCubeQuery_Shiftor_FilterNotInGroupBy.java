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
package eu.solven.adhoc.query.column_shift;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.measure.sum.SubstractionCombination;
import eu.solven.adhoc.query.cube.CubeQuery;

/**
 * Reproducer for the regression introduced by
 * {@code "Ensure EmptyAggregator are registered only if current slice passes their filter"}: when a {@link Shiftor}'s
 * shifted FILTER references a column that is NOT in the query's groupBy, the empty marker is no longer materialized and
 * the shifted measure ends up null, breaking day-over-day style measures.
 *
 * <p>
 * Shape of the bug, mirroring {@code TestTransformator_Shiftor_Perf#testGroupByRow_Today} but with InMemoryTable so we
 * can iterate on the fix without a benchmark/DuckDB harness:
 * <ul>
 * <li>{@code k1Sum} aggregates {@code k1}.</li>
 * <li>{@code prev} is a Shiftor reading {@code k1Sum} with FILTER edited from {@code d=today} to
 * {@code d=yesterday}.</li>
 * <li>{@code dToD = k1Sum - prev}.</li>
 * <li>The query groups by {@code row_index} and filters on {@code d=today} — so the slice arriving at the engine's
 * empty-marker materialization is {@code {row_index=0}}, NOT carrying {@code d}.</li>
 * </ul>
 *
 * <p>
 * In {@code TabularRecordStreamReducer.forEachMeasure}, the empty aggregator's FILTER {@code d=today} is then evaluated
 * against {@code {row_index=0}}: the column is missing, the default {@code IF_MISSING_AS_NULL} treats it as
 * non-matching, and the marker is dropped. The Shiftor never materializes its slice and {@code dToD} reads
 * {@code today_value - null = today_value} instead of {@code today_value - yesterday_value}.
 */
public class TestCubeQuery_Shiftor_FilterNotInGroupBy extends ADagTest implements IAdhocTestConstants {

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("d", "today", "row_index", 0L, "k1", 9));
		table().add(Map.of("d", "today", "row_index", 1L, "k1", 10));
		table().add(Map.of("d", "yesterday", "row_index", 0L, "k1", 4));
		table().add(Map.of("d", "yesterday", "row_index", 1L, "k1", 5));
	}

	/**
	 * {@link IFilterEditor} that rewrites {@code d=today} into {@code d=yesterday}, mirroring the
	 * {@code PreviousDayFilterEditor} of the perf test but on a string column to keep the unit-test independent of any
	 * date arithmetic.
	 */
	public static class TodayToYesterdayShifter implements IFilterEditor {
		@Override
		public ISliceFilter editFilter(ISliceFilter input) {
			return SimpleFilterEditor.shift(input, "d", "yesterday");
		}
	}

	String prev = "prev";
	String dToD = "dayToDay";

	@BeforeEach
	void prepareMeasures() {
		forest.addMeasure(k1Sum);

		forest.addMeasure(Shiftor.builder()
				.name(prev)
				.underlying(k1Sum.getName())
				.editorKey(TodayToYesterdayShifter.class.getName())
				.build());

		forest.addMeasure(Combinator.builder()
				.name(dToD)
				.underlyings(List.of(k1Sum.getName(), prev))
				.combinationKey(SubstractionCombination.KEY)
				.build());
	}

	/**
	 * Sanity: groupBy includes {@code d}, so the slice carries the FILTER column and the existing logic works.
	 */
	@Test
	public void testGroupByDate_filterColumnInGroupBy() {
		ITabularView output = cube().execute(
				CubeQuery.builder().measure(dToD).groupByAlso("row_index", "d").andFilter("d", "today").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("row_index", 0L, "d", "today"), Map.of(dToD, 0L + 9 - 4))
				.containsEntry(Map.of("row_index", 1L, "d", "today"), Map.of(dToD, 0L + 10 - 5));
	}

	/**
	 * Reproducer: groupBy is only {@code row_index}, FILTER on {@code d=today} stays in the WHERE clause. The slice
	 * arriving at the engine no longer carries {@code d}, the empty marker is silently dropped, and {@code dToD}
	 * collapses to {@code k1Sum} instead of {@code k1Sum - prev}.
	 *
	 * <p>
	 * Expected (when the bug is fixed): {@code dToD = 9 - 4 = 5} for {@code row_index=0}, {@code 10 - 5 = 5} for
	 * {@code row_index=1}.
	 *
	 * <p>
	 * Actual (current regression): {@code dToD = 9} and {@code 10} respectively (Shiftor returns null).
	 */
	@Test
	public void testGroupByRow_filterColumnNotInGroupBy() {
		ITabularView output = cube()
				.execute(CubeQuery.builder().measure(dToD).groupByAlso("row_index").andFilter("d", "today").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("row_index", 0L), Map.of(dToD, 0L + 9 - 4))
				.containsEntry(Map.of("row_index", 1L), Map.of(dToD, 0L + 10 - 5));
	}
}
