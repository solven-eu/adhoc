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
import eu.solven.adhoc.column.calculated.ICalculatedColumn;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.factories.CustomMarkerScope;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.model.column.FunctionCalculatedColumn;
import eu.solven.adhoc.model.measure.Aggregator;

/**
 * End-to-end test that an {@link ICalculatedColumn} can read the executing query's {@code customMarker} via
 * {@link CustomMarkerScope#current()} during {@code computeCoordinate} — without {@link ICalculatedColumn} having any
 * customMarker parameter on its API. This is the motivating use case for {@link CustomMarkerScope}: extension points
 * vary their behaviour based on the query context, while the engine threads no extra arguments through every internal
 * type.
 *
 * @author Benoit Lacelle
 */
public class TestDagCubeQuery_CalculatedColumn_CustomMarkerScope extends ATestDagInMemory
		implements IAdhocTestConstants {

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("a", "a1", "k1", 100));
		table().add(Map.of("a", "a2", "k1", 200));

		forest.addMeasure(Aggregator.builder().name("k1").columnName("k1").aggregationKey(SumAggregation.KEY).build());
	}

	// Reads the query's customMarker via CustomMarkerScope#current and labels every row with it. A calculated column
	// configured this way would normally have no way to vary its output per query — CustomMarkerScope is the seam.
	protected ICalculatedColumn markerAwareColumn() {
		return FunctionCalculatedColumn.builder()
				.name("scope_marker")
				.recordToCoordinate(_ -> CustomMarkerScope.current().map(Object::toString).orElse("absent"))
				.build();
	}

	@Test
	public void testCalculatedColumn_readsCustomMarkerFromScope() {
		ITabularView view = cube().execute(
				CubeQuery.builder().measure("k1").groupByAlso(markerAwareColumn()).customMarker("ccy=EUR").build());

		// The function ran inside a CubeQueryEngine scope bound to "ccy=EUR" — every row carries that label.
		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
				.containsEntry(Map.of("scope_marker", "ccy=EUR"), Map.of("k1", 0L + 100 + 200))
				.hasSize(1);
	}

	@Test
	public void testCalculatedColumn_noCustomMarker_readsEmptyFromScope() {
		ITabularView view = cube().execute(CubeQuery.builder().measure("k1").groupByAlso(markerAwareColumn()).build());

		// Engine still binds the scope, but with a null marker — current() returns empty, function returns "absent".
		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
				.containsEntry(Map.of("scope_marker", "absent"), Map.of("k1", 0L + 100 + 200))
				.hasSize(1);
	}

	@Test
	public void testCustomMarkerScope_isUnboundOutsideEngineExecute() {
		// Outside a CubeQueryEngine#execute call, no scope is active — the helper degrades gracefully rather than
		// throwing. Pinning this so a future refactor that makes #current throw on missing binding is caught.
		Assertions.assertThat(CustomMarkerScope.current()).isEmpty();
		Assertions.assertThat(CustomMarkerScope.isBound()).isFalse();
	}
}
