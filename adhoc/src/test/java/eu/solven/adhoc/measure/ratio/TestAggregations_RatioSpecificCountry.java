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
package eu.solven.adhoc.measure.ratio;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.measure.examples.RatioOverSpecificColumnValueCompositor;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;
import lombok.extern.slf4j.Slf4j;

/**
 * Test a ratio a defined by {@link RatioOverSpecificColumnValueCompositor}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class TestAggregations_RatioSpecificCountry extends ADagTest {

	@Override
	@BeforeEach
	public void feedTable() {
		rows.add(Map.of("country", "FR", "city", "Paris", "d", 123, "color", "blue"));
		rows.add(Map.of("country", "FR", "city", "Lyon", "d", 234, "color", "green"));
		rows.add(Map.of("country", "DE", "city", "Berlin", "d", 345, "color", "red"));
		rows.add(Map.of("country", "US", "city", "Paris", "d", 456, "color", "blue"));
		rows.add(Map.of("country", "US", "city", "New-York", "d", 567, "color", "green"));
	}

	@BeforeEach
	public void registerMeasures() {
		amb.acceptVisitor(new RatioOverSpecificColumnValueCompositor().asCombinator("country", "FR", "d"));

		amb.addMeasure(Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build());

		amb.getNameToMeasure().forEach((measureName, measure) -> {
			log.debug("Measure: {}", measureName);
		});
	}

	@Test
	public void testGrandTotal() {
		AdhocQuery adhocQuery = AdhocQuery.builder().measure("d_country=FR_ratio").build();
		ITabularView output = aqw.execute(adhocQuery);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("d_country=FR_ratio", (0D + 456 + 567) / (0D + 456 + 567)));
	}

	@Test
	public void testFR() {
		AdhocQuery adhocQuery = AdhocQuery.builder().measure("d_country=FR_ratio").andFilter("country", "FR").build();
		ITabularView output = aqw.execute(adhocQuery);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("d_country=FR_ratio", (0D + 456 + 567) / (456 + 567)));
	}

	@Test
	public void testParis() {
		AdhocQuery adhocQuery = AdhocQuery.builder().measure("d_country=FR_ratio").andFilter("city", "Paris").build();
		ITabularView output = aqw.execute(adhocQuery);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("d_country=FR_ratio", (0D + 123) / (123 + 234)));
	}

	@Test
	public void testUS() {
		AdhocQuery adhocQuery =
				AdhocQuery.builder().measure("d", "d_country=FR_ratio").andFilter("country", "US").build();
		ITabularView output = aqw.execute(adhocQuery);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("d", 0L + 456 + 567));
	}

	@Test
	public void testExplain_filterOtherColumn() {
		List<String> messages = AdhocExplainerTestHelper.listenForExplain(eventBus);

		{
			AdhocQuery adhocQuery =
					AdhocQuery.builder().measure("d_country=FR_ratio").andFilter("color", "blue").explain(true).build();
			aqw.execute(adhocQuery);
		}

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n"))).isEqualTo("""
				#0 m=d_country=FR_ratio(Combinator) filter=color=blue groupBy=grandTotal
				|\\- #1 m=d_country=FR_slice(Filtrator) filter=color=blue groupBy=grandTotal
				|   \\-- #2 m=d(Aggregator) filter=color=blue&country=FR groupBy=grandTotal
				\\-- #3 m=d_country=FR_whole(Unfiltrator) filter=color=blue groupBy=grandTotal
				    \\-- #4 m=d_country=FR_slice(Filtrator) filter=matchAll groupBy=grandTotal
				        \\-- #5 m=d(Aggregator) filter=country=FR groupBy=grandTotal
																  		""".trim());

		Assertions.assertThat(messages).hasSize(6);
	}
}
