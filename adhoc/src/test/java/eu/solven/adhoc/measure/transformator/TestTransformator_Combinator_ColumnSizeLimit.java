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
package eu.solven.adhoc.measure.transformator;

import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.model.Bucketor;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.util.AdhocUnsafe;

public class TestTransformator_Combinator_ColumnSizeLimit extends ADagTest implements IAdhocTestConstants {

	@BeforeEach
	public void setLimitTo2() {
		System.setProperty("adhoc.limitColumnSize", "2");
		AdhocUnsafe.reloadProperties();
	}

	@AfterEach
	public void resetLimit() {
		System.clearProperty("adhoc.limitColumnSize");
		AdhocUnsafe.reloadProperties();
	}

	@Override
	@BeforeEach
	public void feedTable() {
		table.add(Map.of("k", "a"));
		table.add(Map.of("k", "b"));
		table.add(Map.of("k", "c"));

		forest.addMeasure(countAsterisk);

		forest.addMeasure(Bucketor.builder()
				.name("byK")
				.underlying(countAsterisk.getName())
				.groupBy(GroupByColumns.named("k"))
				.combinationKey(CoalesceCombination.KEY)
				.build());
	}

	@Test
	public void testGrandTotal() {
		ITabularView output = cube().execute(CubeQuery.builder().measure(countAsterisk.getName()).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(countAsterisk.getName(), 3L));
	}

	@Test
	public void testGroupByK_countAggregator() {
		Assertions.setMaxStackTraceElementsDisplayed(300);

		Assertions.assertThatThrownBy(
				() -> cube().execute(CubeQuery.builder().groupByAlso("k").measure(countAsterisk.getName()).build()))
				.isInstanceOf(IllegalStateException.class)
				.hasRootCauseMessage("Can not add as size=2 and limit=2");
	}

	@Test
	public void testGroupByK_noAggregator() {
		Assertions.setMaxStackTraceElementsDisplayed(300);

		Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder().groupByAlso("k").build()))
				.isInstanceOf(IllegalStateException.class)
				.hasRootCauseMessage("Can not add as size=2 and limit=2");
	}

	@Test
	public void testBucketorByK() {
		Assertions.setMaxStackTraceElementsDisplayed(300);

		Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder().measure("byK").build()))
				.isInstanceOf(IllegalStateException.class)
				.hasRootCauseInstanceOf(IllegalStateException.class)
				.hasRootCauseMessage("Can not add as size=2 and limit=2");
	}

}
