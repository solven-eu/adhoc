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
package eu.solven.adhoc.measure;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.ITabularView;
import eu.solven.adhoc.MapBasedTabularView;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.aggregations.sum.SumCombination;
import eu.solven.adhoc.database.InMemoryDatabase;
import eu.solven.adhoc.database.PrefixTranscoder;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.Combinator;

public class TestAggregations_Transcoding extends ADagTest {
	public final InMemoryDatabase rows =
			InMemoryDatabase.builder().transcoder(PrefixTranscoder.builder().prefix("p_").build()).build();

	@Override
	@BeforeEach
	public void feedDb() {
		// As assume the data in DB is already prefixed with `_p`
		rows.add(Map.of("p_c", "v1", "p_k1", 123D));
		rows.add(Map.of("p_c", "v2", "p_k2", 234D));

		amb.addMeasure(Combinator.builder()
				.name("sumK1K2")
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build());

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build());
	}

	@Test
	public void testGrandTotal() {
		ITabularView output = aqe.execute(AdhocQuery.builder().measure("sumK1K2").debug(true).build(), rows);

		List<Map<String, ?>> keySet = output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0D + 123 + 234));
	}

	@Test
	public void testFilter() {
		ITabularView output = aqe.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("c", "v1").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0D + 123));
	}

	@Test
	public void testGroupBy() {
		ITabularView output = aqe.execute(AdhocQuery.builder().measure("sumK1K2").groupByColumns("c").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(2).contains(Map.of("c", "v1"), Map.of("c", "v2"));

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("c", "v1"), Map.of("sumK1K2", 0D + 123))
				.containsEntry(Map.of("c", "v2"), Map.of("sumK1K2", 0D + 234));
	}

	@Test
	public void testFilterGroupBy() {
		ITabularView output = aqe.execute(
				AdhocQuery.builder().measure("sumK1K2").andFilter("c", "v1").groupByColumns("c").debug(true).build(),
				rows);

		List<Map<String, ?>> keySet = output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Map.of("c", "v1"));

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of("c", "v1"), Map.of("sumK1K2", 0D + 123));
	}
}
