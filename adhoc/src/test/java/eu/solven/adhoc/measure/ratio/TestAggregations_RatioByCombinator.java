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
import eu.solven.adhoc.aggregations.DivideCombination;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.view.ITabularView;
import eu.solven.adhoc.view.MapBasedTabularView;
import lombok.extern.slf4j.Slf4j;

/**
 * One specific thing in this unitTest is that `FRoverUS` calls twice the same underlying measure (but with different
 * filters).
 *
 * It also demonstrate another case of custom IMeasure: we introduced {@link RatioByCombinator} as a custom
 * {@link IMeasure}.
 */
@Slf4j
public class TestAggregations_RatioByCombinator extends ADagTest {

	@Override
	@BeforeEach
	public void feedDb() {
		rows.add(Map.of("country", "FR", "city", "Paris", "d", 123, "color", "blue"));
		rows.add(Map.of("country", "FR", "city", "Lyon", "d", 234, "color", "green"));
		rows.add(Map.of("country", "DE", "city", "Berlin", "d", 345, "color", "red"));
		rows.add(Map.of("country", "US", "city", "Paris", "d", 456, "color", "blue"));
		rows.add(Map.of("country", "US", "city", "New-York", "d", 567, "color", "green"));
	}

	@BeforeEach
	public void registerMeasures() {
		amb.addMeasure(RatioByCombinator.builder()
				.name("FRoverUS")
				.underlying("d")
				.combinationKey(DivideCombination.KEY)
				.numeratorFilter(ColumnFilter.isEqualTo("country", "FR"))
				.denominatorFilter(ColumnFilter.isEqualTo("country", "US"))
				.build());

		amb.addMeasure(Aggregator.builder().name("d").aggregationKey(SumAggregator.KEY).build());

		amb.getNameToMeasure().forEach((measureName, measure) -> {
			log.debug("Measure: {}", measureName);
		});
	}

	@Test
	public void testGrandTotal() {
		AdhocQuery adhocQuery = AdhocQuery.builder().measure("FRoverUS").debug(true).build();
		ITabularView output = aqw.execute(adhocQuery);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("FRoverUS", (0D + 123 + 234) / (0D + 456 + 567)));
	}

	@Test
	public void testFR() {
		AdhocQuery adhocQuery = AdhocQuery.builder().measure("FRoverUS").andFilter("country", "FR").debug(true).build();
		ITabularView output = aqw.execute(adhocQuery);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		// NaN as we divide by US, hence dividing by 0 (as there is no row in both FR and US)
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("FRoverUS", Double.NaN));
	}

	@Test
	public void testWildcardCountry() {
		AdhocQuery adhocQuery = AdhocQuery.builder().measure("FRoverUS").groupByAlso("country").debug(true).build();
		ITabularView output = aqw.execute(adhocQuery);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				// On FR, US denominator is null
				.containsEntry(Map.of("country", "FR"), Map.of("FRoverUS", Double.NaN))
		// On US, FR numerator is null
		// .containsEntry(Map.of("country", "US"), Map.of("FRoverUS", 0D))
		;
	}

	@Test
	public void testParis() {
		AdhocQuery adhocQuery = AdhocQuery.builder().measure("FRoverUS").andFilter("city", "Paris").debug(true).build();
		ITabularView output = aqw.execute(adhocQuery);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("FRoverUS", (0D + 123) / (0D + 456)));
	}

	@Test
	public void testWildcardCountry_Paris() {
		AdhocQuery adhocQuery = AdhocQuery.builder()
				.measure("FRoverUS")
				.groupByAlso("country")
				.andFilter("city", "Paris")
				.debug(true)
				.build();
		ITabularView output = aqw.execute(adhocQuery);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				// On FR, US denominator is null
				.containsEntry(Map.of("country", "FR"), Map.of("FRoverUS", Double.NaN))
		// On US, FR numerator is null
		// .containsEntry(Map.of("country", "US"), Map.of("FRoverUS", 0D))
		;
	}

	@Test
	public void testUS() {
		AdhocQuery adhocQuery =
				AdhocQuery.builder().measure("d", "FRoverUS").andFilter("country", "US").debug(true).build();
		ITabularView output = aqw.execute(adhocQuery);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("d", 0L + 456 + 567
				// On US, FR numerator is null
				// , "FRoverUS", 0D
				));
	}

	@Test
	public void testExplain_groupByGroups() {
		List<String> messages = AdhocExplainerTestHelper.listenForExplain(eventBus);

		{
			AdhocQuery adhocQuery =
					AdhocQuery.builder().measure("d", "FRoverUS").andFilter("country", "US").explain(true).build();
			aqw.execute(adhocQuery);
		}

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n"))).isEqualTo("""
				#0 m=FRoverUS(RatioByCombinator) filter=country=US groupBy=grandTotal
				|\\- #1 m=d(Aggregator) filter=matchNone groupBy=grandTotal
				\\-- #2 m=d(Aggregator) filter=country=US groupBy=grandTotal
				!2
												  		""".trim());

		Assertions.assertThat(messages).hasSize(4);
	}
}
