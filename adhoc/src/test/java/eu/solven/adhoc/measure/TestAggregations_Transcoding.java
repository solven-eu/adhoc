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
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.cube.AdhocCubeWrapper;
import eu.solven.adhoc.measure.step.Aggregator;
import eu.solven.adhoc.measure.step.Combinator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;
import eu.solven.adhoc.table.InMemoryTable;
import eu.solven.adhoc.table.transcoder.PrefixTranscoder;

public class TestAggregations_Transcoding extends ADagTest {
	public final InMemoryTable rows =
			InMemoryTable.builder().transcoder(PrefixTranscoder.builder().prefix("p_").build()).build();
	public final AdhocCubeWrapper aqw = AdhocCubeWrapper.builder().table(rows).engine(aqe).measures(amb).build();

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

		amb.addMeasure(Aggregator.builder().name("k1").aggregationKey(SumAggregation.KEY).build());
		amb.addMeasure(Aggregator.builder().name("k2").aggregationKey(SumAggregation.KEY).build());
	}

	@Test
	public void testGrandTotal() {
		ITabularView output = aqw.execute(AdhocQuery.builder().measure("sumK1K2").debug(true).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0D + 123 + 234));
	}

	@Test
	public void testFilter() {
		ITabularView output = aqw.execute(AdhocQuery.builder().measure("sumK1K2").andFilter("c", "v1").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("sumK1K2", 0D + 123));
	}

	@Test
	public void testGroupBy() {
		ITabularView output = aqw.execute(AdhocQuery.builder().measure("sumK1K2").groupByAlso("c").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("c", "v1"), Map.of("sumK1K2", 0D + 123))
				.containsEntry(Map.of("c", "v2"), Map.of("sumK1K2", 0D + 234));
	}

	@Test
	public void testFilterGroupBy() {
		ITabularView output = aqw.execute(
				AdhocQuery.builder().measure("sumK1K2").andFilter("c", "v1").groupByAlso("c").debug(true).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of("c", "v1"), Map.of("sumK1K2", 0D + 123));
	}
}
