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
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.examples.RatioOverCurrentColumnValueCompositor;
import eu.solven.adhoc.view.ITabularView;
import eu.solven.adhoc.view.MapBasedTabularView;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestAggregations_RatioCurrentCountry extends ADagTest {

	@Override
	@BeforeEach
	public void feedDb() {
		rows.add(Map.of("country", "USA", "city", "Paris", "d", 123, "color", "blue"));
		rows.add(Map.of("country", "USA", "city", "New-York", "d", 234, "color", "green"));
		rows.add(Map.of("country", "FRANCE", "city", "Paris", "d", 456, "color", "blue"));
		rows.add(Map.of("country", "FRANCE", "city", "Lyon", "d", 567, "color", "green"));
	}

	@BeforeEach
	public void registerMeasures() {
		amb.acceptMeasureCombinator(new RatioOverCurrentColumnValueCompositor().asCombinator("country", "d"));

		amb.addMeasure(Aggregator.builder().name("d").aggregationKey(SumAggregator.KEY).build());

		amb.getNameToMeasure().forEach((measureName, measure) -> {
			log.debug("Measure: {}", measureName);
		});
	}

	@Test
	public void testGrandTotal() {
		AdhocQuery adhocQuery = AdhocQuery.builder().measure("d_country=current_valid").debug(true).build();
		ITabularView output = aqw.execute(adhocQuery);

		// List<Map<String, ?>> keySet =
		// output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		// Assertions.assertThat(keySet).hasSize(0);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(0);
	}

	@Test
	public void testFrance() {
		AdhocQuery adhocQuery = AdhocQuery.builder()
				.measure("d_country=current_valid")
				.andFilter("country", "FRANCE")
				.debug(true)
				.build();
		ITabularView output = aqw.execute(adhocQuery);

		// List<Map<String, ?>> keySet =
		// output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		// Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("d_country=current_valid", (0D + 456 + 567) / (0D + 456 + 567)));
	}

	@Test
	public void testWildcardCountry() {
		AdhocQuery adhocQuery =
				AdhocQuery.builder().measure("d_country=current_valid").groupByAlso("country").debug(true).build();
		ITabularView output = aqw.execute(adhocQuery);

		// List<Map<String, ?>> keySet =
		// output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		// Assertions.assertThat(keySet).hasSize(2).contains(Map.of("country", "FRANCE"), Map.of("country", "USA"));

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("country", "FRANCE"),
						Map.of("d_country=current_valid", (0D + 456 + 567) / (0D + 456 + 567)))
				.containsEntry(Map.of("country", "USA"),
						Map.of("d_country=current_valid", (0D + 456 + 567) / (0D + 456 + 567)));
	}

	@Test
	public void testParis() {
		AdhocQuery adhocQuery =
				AdhocQuery.builder().measure("d_country=current_valid").andFilter("city", "Paris").debug(true).build();
		ITabularView output = aqw.execute(adhocQuery);

		// List<Map<String, ?>> keySet =
		// output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		// Assertions.assertThat(keySet).hasSize(0);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(0);
	}

	@Test
	public void testWildcardCountry_Paris() {
		AdhocQuery adhocQuery = AdhocQuery.builder()
				.measure("d_country=current_valid")
				.groupByAlso("country")
				.andFilter("city", "Paris")
				.debug(true)
				.build();
		ITabularView output = aqw.execute(adhocQuery);

		// List<Map<String, ?>> keySet =
		// output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		// Assertions.assertThat(keySet).hasSize(2).contains(Map.of("country", "FRANCE"), Map.of("country", "USA"));

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("country", "FRANCE"),
						Map.of("d_country=current_valid", (0D + 456) / (0D + 456 + 567)))
				.containsEntry(Map.of("country", "USA"),
						Map.of("d_country=current_valid", (0D + 123) / (0D + 123 + 234)));
	}

	@Test
	public void testUSA() {
		AdhocQuery adhocQuery = AdhocQuery.builder()
				.measure("d", "d_country=current_valid")
				.andFilter("country", "USA")
				.debug(true)
				.build();
		ITabularView output = aqw.execute(adhocQuery);

		// List<Map<String, ?>> keySet =
		// output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		// Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("d", 0L + 123 + 234, "d_country=current_valid", (0D + 123 + 234) / (0D + 123 + 234)));
	}

	@Test
	public void testExplain_groupByGroups() {
		List<String> messages = AdhocExplainerTestHelper.listenForExplain(eventBus);

		{
			AdhocQuery adhocQuery = AdhocQuery.builder()
					.measure("d_country=current_valid")
					.andFilter("country", "US")
					.debug(true)
					.build();
			aqw.execute(adhocQuery);
		}

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n"))).isEqualTo("""
				m=d_country=current_valid(Columnator) filter=country=US groupBy=grandTotal
				\\-- m=d_country=current_ratio(Combinator) filter=country=US groupBy=grandTotal
				    |\\- m=d_country=current_slice(Bucketor) filter=country=US groupBy=grandTotal
				    |   \\-- m=d(Aggregator) filter=country=US groupBy=(country)
				    \\-- m=d_country=current_whole(Unfiltrator) filter=country=US groupBy=grandTotal
												  		""".trim());

		Assertions.assertThat(messages).hasSize(5);
	}
}
