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
package eu.solven.adhoc.column;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ATestDagInMemory;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.column.coordinate.ICalculatedCoordinate;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.options.CustomMarkerScope;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.model.column.ColumnWithCalculatedCoordinates;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;

/**
 * End-to-end test that an {@link ICalculatedCoordinate}'s {@code getFilter()} can read the executing query's
 * {@code customMarker} via {@link CustomMarkerScope#current()} during planning — without {@link ICalculatedCoordinate}
 * having any customMarker parameter on its API.
 *
 * <p>
 * The calc-coord's filter is consulted by {@code QueryStepsDagBuilder.rootMeasureless} at planning time, BEFORE row
 * processing. That is why
 * {@link eu.solven.adhoc.engine.CubeQueryEngine#execute(eu.solven.adhoc.engine.context.QueryPod)} binds the
 * {@link CustomMarkerScope} around the whole call (planning + execution) — see {@code executeInScope}.
 *
 * @author Benoit Lacelle
 */
public class TestDagCubeQuery_CalculatedCoordinate_CustomMarkerScope extends ATestDagInMemory
		implements IAdhocTestConstants {

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("a", "a1", "k1", 100));
		table().add(Map.of("a", "a2", "k1", 200));

		forest.addMeasure(Aggregator.builder().name("k1").columnName("k1").aggregationKey(SumAggregation.KEY).build());
	}

	// Custom calc coord whose filter is decided at getFilter() time by reading CustomMarkerScope. With marker
	// "a1_only" the filter narrows to a=a1; without the marker the filter is MATCH_ALL. The synthetic coordinate
	// value is the literal "scoped" so it does not collide with any natural `a` value (no NOT IN suppression kicks in).
	protected ICalculatedCoordinate markerAwareCoord() {
		return new ICalculatedCoordinate() {
			@Override
			public Object getCoordinate() {
				return "scoped";
			}

			@Override
			public ISliceFilter getFilter() {
				Object marker = CustomMarkerScope.current().orElse(null);
				if ("a1_only".equals(marker)) {
					return ColumnFilter.matchEq("a", "a1");
				}
				return ISliceFilter.MATCH_ALL;
			}
		};
	}

	@Test
	public void testCalculatedCoordinate_filterReadsCustomMarkerFromScope_a1Only() {
		ITabularView view = cube().execute(CubeQuery.builder()
				.measure("k1")
				.groupBy(GroupByColumns.of(ColumnWithCalculatedCoordinates.builder()
						.column("a")
						.calculatedCoordinate(markerAwareCoord())
						.build()))
				.customMarker("a1_only")
				.build());

		// Calc coord's filter resolved to a=a1 at planning time -> scoped row only sees a1's k1.
		// Natural `a` rows are unaffected (NOT IN ("scoped") matches nothing real).
		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
				.containsEntry(Map.of("a", "scoped"), Map.of("k1", 0L + 100))
				.containsEntry(Map.of("a", "a1"), Map.of("k1", 0L + 100))
				.containsEntry(Map.of("a", "a2"), Map.of("k1", 0L + 200))
				.hasSize(3);
	}

	@Test
	public void testCalculatedCoordinate_filterReadsCustomMarkerFromScope_noMarker_matchesAll() {
		ITabularView view = cube().execute(CubeQuery.builder()
				.measure("k1")
				.groupBy(GroupByColumns.of(ColumnWithCalculatedCoordinates.builder()
						.column("a")
						.calculatedCoordinate(markerAwareCoord())
						.build()))
				.build());

		// No marker -> calc coord filter is MATCH_ALL -> scoped row sums all rows.
		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
				.containsEntry(Map.of("a", "scoped"), Map.of("k1", 0L + 100 + 200))
				.containsEntry(Map.of("a", "a1"), Map.of("k1", 0L + 100))
				.containsEntry(Map.of("a", "a2"), Map.of("k1", 0L + 200))
				.hasSize(3);
	}

	@Test
	public void testCalculatedCoordinate_filterReadsCustomMarkerFromScope_unrelatedMarker_matchesAll() {
		ITabularView view = cube().execute(CubeQuery.builder()
				.measure("k1")
				.groupBy(GroupByColumns.of(ColumnWithCalculatedCoordinates.builder()
						.column("a")
						.calculatedCoordinate(markerAwareCoord())
						.build()))
				.customMarker("ccy=EUR")
				.build());

		// Marker present but not the sentinel -> calc coord falls through to MATCH_ALL.
		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
				.containsEntry(Map.of("a", "scoped"), Map.of("k1", 0L + 100 + 200))
				.containsEntry(Map.of("a", "a1"), Map.of("k1", 0L + 100))
				.containsEntry(Map.of("a", "a2"), Map.of("k1", 0L + 200))
				.hasSize(3);
	}
}
