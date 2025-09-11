/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.transcoder.MapTableAliaser;
import eu.solven.adhoc.util.NotYetImplementedException;

public class TestCubeQuery_CalculatedColumn extends ADagTest implements IAdhocTestConstants {
	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("a", "a1", "k1", 123));
		table().add(Map.of("a", "a1", "k1", 345, "k2", 456));
		table().add(Map.of("a", "a2", "b", "b1", "k2", 234));
		table().add(Map.of("a", "a2", "b", "b2", "k1", 567));

		// This first `k1` overlaps with the columnName
		forest.addMeasure(Aggregator.builder().name("k1").columnName("k1").aggregationKey(SumAggregation.KEY).build());
		// This second `k1.SUM` does not overlap with the columnName
		forest.addMeasure(
				Aggregator.builder().name("k1.sum").columnName("k1").aggregationKey(SumAggregation.KEY).build());
	}

	@Test
	public void test_groupBy_definitionInQuery() {
		ITabularView view = cube().execute(CubeQuery.builder()
				.measure("k1")
				.groupByAlso(FunctionCalculatedColumn.builder()
						.name("custom")
						.recordToCoordinate(r -> r.getGroupBy("a") + "-" + r.getGroupBy("b"))
						.build())
				.build());

		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
				.containsEntry(Map.of("custom", "a1-null"), Map.of("k1", 0L + 123 + 345))
				.containsEntry(Map.of("custom", "a2-b2"), Map.of("k1", 0L + 567))
				.hasSize(2);
	}

	@Test
	public void test_groupBy_definitionInCubeColumnManager() {
		CubeWrapper cube = editCube().columnsManager(ColumnsManager.builder()
				.calculatedColumn(FunctionCalculatedColumn.builder()
						.name("custom")
						.recordToCoordinate(r -> r.getGroupBy("a") + "-" + r.getGroupBy("b"))
						.build())
				.build()).build();

		ITabularView view = cube.execute(CubeQuery.builder().measure("k1").groupByAlso("custom").build());

		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
				.containsEntry(Map.of("custom", "a1-null"), Map.of("k1", 0L + 123 + 345))
				.containsEntry(Map.of("custom", "a2-b2"), Map.of("k1", 0L + 567))
				.hasSize(2);
	}

	@Test
	public void test_groupBy_filter() {
		Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder()
				.measure("k1")
				.groupByAlso(FunctionCalculatedColumn.builder()
						.name("custom")
						.recordToCoordinate(r -> r.getGroupBy("a") + "-" + r.getGroupBy("b"))
						.build())
				.andFilter("custom", "a2-b2")
				.build())).hasRootCauseInstanceOf(NotYetImplementedException.class);
	}

	@Test
	public void test_groupBy_rename() {
		CubeWrapper cube = editCube().columnsManager(ColumnsManager.builder()
				.aliaser(MapTableAliaser.builder()
						.aliasToOriginal("proxy_a", "a")
						.aliasToOriginal("proxy_b", "b")
						.build())
				.build()).build();

		ITabularView view = cube.execute(CubeQuery.builder()
				.measure("k1")
				.groupByAlso(FunctionCalculatedColumn.builder()
						.name("custom")
						.recordToCoordinate(r -> r.getGroupBy("proxy_a") + "-" + r.getGroupBy("proxy_b"))
						.build())
				.build());

		Assertions.assertThat(MapBasedTabularView.load(view).getCoordinatesToValues())
				.containsEntry(Map.of("custom", "a1-null"), Map.of("k1", 0L + 123 + 345))
				.containsEntry(Map.of("custom", "a2-b2"), Map.of("k1", 0L + 567))
				.hasSize(2);
	}
}
