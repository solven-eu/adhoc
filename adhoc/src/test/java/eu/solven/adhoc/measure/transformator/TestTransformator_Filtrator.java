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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.sum.DivideCombination;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;

public class TestTransformator_Filtrator extends ADagTest implements IAdhocTestConstants {

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("a", "a1", "k1", 123));
		table().add(Map.of("a", "a2", "b", "b1", "k2", 234));
		table().add(Map.of("a", "a1", "k1", 345, "k2", 456));
		table().add(Map.of("a", "a2", "b", "b2", "k1", 567));
	}

	@Test
	public void testSumOfSum_filterA1() {
		forest

				.addMeasure(Filtrator.builder()
						.name("filterK1onA1")
						.underlying("k1")
						.filter(ColumnFilter.isEqualTo("a", "a1"))
						.build())

				.addMeasure(Filtrator.builder()
						.name("filterK1onA2")
						.underlying("k1")
						.filter(ColumnFilter.builder().column("a").matching("a2").build())
						.build())

				.addMeasure(Combinator.builder()
						.name("Ratio_k1_k1witha1")
						.underlyings(Arrays.asList("k1", "filterK1onA1"))
						.combinationKey(DivideCombination.KEY)
						.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		CubeQuery adhocQuery = CubeQuery.builder().measure("k1", "filterK1onA1").build();
		ITabularView output = cube().execute(adhocQuery);

		// List<Map<String, ?>> keySet =
		// output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		// Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("k1", 0L + 123 + 345 + 567, "filterK1onA1", 0L + 123 + 345));
	}

	@Test
	public void testSumOfSum_filterA1_divide() {
		forest

				.addMeasure(Filtrator.builder()
						.name("filterK1onA1")
						.underlying("k1")
						.filter(ColumnFilter.builder().column("a").matching("a1").build())
						.build())

				.addMeasure(Filtrator.builder()
						.name("filterK1onA2")
						.underlying("k1")
						.filter(ColumnFilter.builder().column("a").matching("a2").build())
						.build())

				.addMeasure(Combinator.builder()
						.name("Ratio_k1_k1witha1")
						.underlyings(Arrays.asList("filterK1onA1", "k1"))
						.combinationKey(DivideCombination.KEY)
						.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		CubeQuery adhocQuery = CubeQuery.builder().measure("Ratio_k1_k1witha1").build();
		ITabularView output = cube().execute(adhocQuery);

		// List<Map<String, ?>> keySet =
		// output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		// Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("Ratio_k1_k1witha1", 1D * (0L + 123 + 345) / (0L + 123 + 345 + 567)));
	}
}
