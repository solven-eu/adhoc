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
package eu.solven.adhoc.query.duplicating;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.decomposition.DuplicatingDecomposition;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.OrFilter;

public class TestTableQuery_DuplicatingDecomposition extends ADagTest implements IAdhocTestConstants {

	final String dispatchedMeasure = "k1.dispatched";

	final String cElement = "country";
	final String cGroup = "country_groups";

	@Override
	@BeforeEach
	public void feedTable() {
		table.add(Map.of("l", "A", cElement, "FR", "k1", 123));
		table.add(Map.of("l", "B", cElement, "FR", "k1", 234));
		table.add(Map.of("l", "C", cElement, "FR", "k1", 345));
	}

	protected String getDecompositionKey() {
		return DuplicatingDecomposition.class.getName();
	}

	@BeforeEach
	void prepareMeasures() {
		forest.addMeasure(Dispatchor.builder()
				.name(dispatchedMeasure)
				.underlying("k1")
				.decompositionKey(getDecompositionKey())
				.decompositionOptions(Map.of("columnToCoordinates",
						ImmutableMap.builder()
								.put("d1", Set.of("a1", "b1", "c1"))
								.put("d2", Set.of("a2", "b2", "c2"))
								.put("d3", Set.of("a3", "b3", "c3"))
								.build()))
				.build());

		forest.addMeasure(k1Sum);
	}

	@Test
	public void testGrandTotal() {
		ITabularView output = makeCube().execute(CubeQuery.builder().measure(dispatchedMeasure).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123 + 234 + 345));
	}

	@Test
	public void testGrandTotal_filterSingleGroup() {
		ITabularView output =
				makeCube().execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter("d1", "a1").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123 + 234 + 345));
	}

	@Test
	public void testGrandTotal_filterUnknownGroup() {
		ITabularView output =
				makeCube().execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter("d1", "unknown").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).isEmpty();
	}

	@Test
	public void testGrandTotal_filterTwoGroups() {
		ITabularView output = makeCube()
				.execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter("d1", Set.of("a1", "a2")).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123 + 234 + 345));
	}

	@Test
	public void testGrandTotal_filterTwoGroups_groupByOtherDuplicated() {
		ITabularView output = makeCube().execute(CubeQuery.builder()
				.measure(dispatchedMeasure)
				.andFilter("d1", Set.of("a1", "a2"))
				.groupByAlso("d2")
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(3)
				.containsEntry(Map.of("d2", "a2"), Map.of(dispatchedMeasure, 0L + 123 + 234 + 345))
				.containsEntry(Map.of("d2", "b2"), Map.of(dispatchedMeasure, 0L + 123 + 234 + 345))
				.containsEntry(Map.of("d2", "c2"), Map.of(dispatchedMeasure, 0L + 123 + 234 + 345));
	}

	@Test
	public void testGrandTotal_filtercomplexOr() {
		ITabularView output = makeCube().execute(CubeQuery.builder()
				.measure(dispatchedMeasure)
				// `l=A&d1=a1|l=B&d2=a2`
				.filter(OrFilter.or(AndFilter.and(Map.of("l", "A", "d1", "a1")),
						AndFilter.and(Map.of("l", "B", "d2", "a2"))))
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of(dispatchedMeasure, 0L + 123 + 234));
	}
	//
	// @Test
	// public void testGrandTotal_filterElementWithSingleGroups() {
	// prepareMeasures();
	//
	// ITabularView output =
	// cube.execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cElement, "CH").build());
	//
	// MapBasedTabularView mapBased = MapBasedTabularView.load(output);
	//
	// Assertions.assertThat(mapBased.getCoordinatesToValues())
	// .hasSize(1)
	// .containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 234));
	// }
	//
	// @Test
	// public void testGrandTotal_elementWithNoGroup() {
	// prepareMeasures();
	//
	// ITabularView output =
	// cube.execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cElement, "CH").build());
	//
	// MapBasedTabularView mapBased = MapBasedTabularView.load(output);
	//
	// Assertions.assertThat(mapBased.getCoordinatesToValues())
	// .hasSize(1)
	// .containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 234));
	// }
	//
	// @Test
	// public void testGrandTotal_filterGroupWithMultipleElements() {
	// prepareMeasures();
	//
	// ITabularView output =
	// cube.execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, "G20").build());
	//
	// MapBasedTabularView mapBased = MapBasedTabularView.load(output);
	//
	// Assertions.assertThat(mapBased.getCoordinatesToValues())
	// .hasSize(1)
	// .containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123 + 234));
	// }
	//
	// @Test
	// public void testGrandTotal_filterGroupWithSingleElement() {
	// prepareMeasures();
	//
	// ITabularView output =
	// cube.execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, "G8").build());
	//
	// MapBasedTabularView mapBased = MapBasedTabularView.load(output);
	//
	// Assertions.assertThat(mapBased.getCoordinatesToValues())
	// .hasSize(1)
	// .containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123));
	// }
	//
	// @Test
	// public void testGrandTotal_filterGroupWithNoElement() {
	// prepareMeasures();
	//
	// ITabularView output =
	// cube.execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, "unknownGroup").build());
	//
	// Assertions.assertThat(output.isEmpty()).isTrue();
	//
	// Assertions.assertThat(output.isEmpty()).isTrue();
	// }
	//
	// @Test
	// public void test_GroupByElement_FilterOneGroup() {
	// prepareMeasures();
	//
	// ITabularView output = cube.execute(
	// CubeQuery.builder().measure(dispatchedMeasure).groupByAlso(cElement).andFilter(cGroup, "G8").build());
	//
	// MapBasedTabularView mapBased = MapBasedTabularView.load(output);
	//
	// Assertions.assertThat(mapBased.getCoordinatesToValues())
	// .hasSize(1)
	// .containsEntry(Map.of(cElement, "FR"), Map.of(dispatchedMeasure, 0L + 123));
	// }
	//
	// @Test
	// public void test_GroupByElement_FilterMultipleGroups() {
	// prepareMeasures();
	//
	// ITabularView output = cube.execute(CubeQuery.builder()
	// .measure(dispatchedMeasure)
	// .groupByAlso(cElement)
	// .andFilter(cGroup, Set.of("G8", "G20"))
	// .build());
	//
	// MapBasedTabularView mapBased = MapBasedTabularView.load(output);
	//
	// Assertions.assertThat(mapBased.getCoordinatesToValues())
	// .hasSize(2)
	// .containsEntry(Map.of(cElement, "FR"), Map.of(dispatchedMeasure, 0L + 123))
	// .containsEntry(Map.of(cElement, "CH"), Map.of(dispatchedMeasure, 0L + 234));
	// }
	//
	// @Test
	// public void test_GroupByGroup_noFilter() {
	// prepareMeasures();
	//
	// ITabularView output = cube.execute(CubeQuery.builder().measure(dispatchedMeasure).groupByAlso(cGroup).build());
	//
	// MapBasedTabularView mapBased = MapBasedTabularView.load(output);
	//
	// Assertions.assertThat(mapBased.getCoordinatesToValues())
	// .hasSize(2)
	// .containsEntry(Map.of(cGroup, "G8"), Map.of(dispatchedMeasure, 0L + 123))
	// .containsEntry(Map.of(cGroup, "G20"), Map.of(dispatchedMeasure, 0L + 123 + 234));
	// }
	//
	// @Test
	// public void test_GroupByGroup_FilterOneElement() {
	// prepareMeasures();
	//
	// ITabularView output = cube.execute(
	// CubeQuery.builder().measure(dispatchedMeasure).groupByAlso(cGroup).andFilter(cElement, "FR").build());
	//
	// MapBasedTabularView mapBased = MapBasedTabularView.load(output);
	//
	// Assertions.assertThat(mapBased.getCoordinatesToValues())
	// .hasSize(2)
	// .containsEntry(Map.of(cGroup, "G8"), Map.of(dispatchedMeasure, 0L + 123))
	// .containsEntry(Map.of(cGroup, "G20"), Map.of(dispatchedMeasure, 0L + 123));
	// }
	//
	// @Test
	// public void test_GroupByGroup_FilterMultipleElements() {
	// prepareMeasures();
	//
	// ITabularView output = cube.execute(CubeQuery.builder()
	// .measure(dispatchedMeasure)
	// .groupByAlso(cGroup)
	// .andFilter(cElement, Set.of("FR", "CH"))
	// .build());
	//
	// MapBasedTabularView mapBased = MapBasedTabularView.load(output);
	//
	// Assertions.assertThat(mapBased.getCoordinatesToValues())
	// .hasSize(2)
	// .containsEntry(Map.of(cGroup, "G8"), Map.of(dispatchedMeasure, 0L + 123))
	// .containsEntry(Map.of(cGroup, "G20"), Map.of(dispatchedMeasure, 0L + 123 + 234));
	// }
	//
	// @Test
	// public void test_NoGroupBy_FilterOneElement() {
	// prepareMeasures();
	//
	// ITabularView output =
	// cube.execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cElement, "FR").build());
	//
	// MapBasedTabularView mapBased = MapBasedTabularView.load(output);
	//
	// Assertions.assertThat(mapBased.getCoordinatesToValues())
	// .hasSize(1)
	// .containsEntry(Map.of(), Map.of(dispatchedMeasure, 0L + 123));
	// }
	//
	// @Test
	// public void test_NoGroupBy_FilterMultipleElements() {
	// prepareMeasures();
	//
	// ITabularView output = cube.execute(
	// CubeQuery.builder().measure(dispatchedMeasure).andFilter(cElement, Set.of("FR", "CH")).build());
	//
	// MapBasedTabularView mapBased = MapBasedTabularView.load(output);
	// Assertions.assertThat(mapBased.getCoordinatesToValues())
	// .hasSize(1)
	// .containsEntry(Map.of(), Map.of(dispatchedMeasure, 0L + 123 + 234));
	// }
	//
	// @Test
	// public void test_NoGroupBy_FilterOneGroup() {
	// prepareMeasures();
	//
	// ITabularView output =
	// cube.execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, "G8").build());
	//
	// MapBasedTabularView mapBased = MapBasedTabularView.load(output);
	//
	// Assertions.assertThat(mapBased.getCoordinatesToValues())
	// .hasSize(1)
	// .containsEntry(Map.of(), Map.of(dispatchedMeasure, 0L + 123));
	// }
	//
	// @Test
	// public void test_NoGroupBy_FilterMultipleGroups() {
	// prepareMeasures();
	//
	// ITabularView output = cube
	// .execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, Set.of("G8", "G20")).build());
	//
	// MapBasedTabularView mapBased = MapBasedTabularView.load(output);
	//
	// Assertions.assertThat(mapBased.getCoordinatesToValues())
	// .hasSize(1)
	// .containsEntry(Map.of(), Map.of(dispatchedMeasure, 0L + 123 + 234));
	// }
	//
	// @Test
	// public void testExplain_groupByGroups() {
	// List<String> messages = AdhocExplainerTestHelper.listenForExplainNoPerf(eventBus);
	//
	// {
	// prepareMeasures();
	// cube.execute(CubeQuery.builder().measure(dispatchedMeasure).groupByAlso(cGroup).explain(true).build());
	// }
	//
	// Assertions.assertThat(messages.stream().collect(Collectors.joining("\n"))).isEqualTo("""
	// #0 s=inMemory id=00000000-0000-0000-0000-000000000000
	// \\-- #1 m=k1.dispatched(Dispatchor[SUM]) filter=matchAll groupBy=(country_groups)
	// \\-- #2 m=k1(SUM) filter=matchAll groupBy=(country)""");
	//
	// Assertions.assertThat(messages).hasSize(3);
	// }
}
