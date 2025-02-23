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
package eu.solven.adhoc.query.foreignexchange;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.AdhocCubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Bucketor;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.measure.sum.SumElseSetAggregation;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;
import lombok.NonNull;

/**
 * This tests a typical financial use-case: given financial data for different currencies, we need to convert the
 * underlying data into a common currency, by a multiplication with the proper rate, depending on current currency. Each
 * data are aggregated by their own currency, then converted into a common currency, and finally aggregated.
 */
public class TestAdhocQueryFx extends ADagTest implements IAdhocTestConstants {

	ForeignExchangeStorage fxStorage = new ForeignExchangeStorage();

	LocalDate today = LocalDate.now();

	public final AdhocQueryEngine aqe = editEngine().operatorsFactory(makeOperatorsFactory(fxStorage)).build();
	public final AdhocCubeWrapper aqw = editCube().engine(aqe).build();

	private @NonNull IOperatorsFactory makeOperatorsFactory(IForeignExchangeStorage fxStorage) {

		return new StandardOperatorsFactory() {
			@Override
			public ICombination makeCombination(String key, Map<String, ?> options) {
				return switch (key) {
				case ForeignExchangeCombination.KEY: {
					yield new ForeignExchangeCombination(fxStorage);
				}
				default:
					yield super.makeCombination(key, options);
				};
			}
		};
	}

	@Override
	@BeforeEach
	public void feedTable() {
		rows.add(Map.of("color", "red", "ccyFrom", "USD", "k1", 123));
		rows.add(Map.of("color", "red", "ccyFrom", "EUR", "k1", 234));
	}

	String mName = "k1.CCY";

	void prepareMeasures() {
		amb.addMeasure(Bucketor.builder()
				.name(mName)
				.underlyings(Arrays.asList(k1Sum.getName()))
				.groupBy(GroupByColumns.named("ccyFrom"))
				.combinationKey(ForeignExchangeCombination.KEY)
				// This tests we can refer an aggregation key but its class
				.aggregationKey(SumElseSetAggregation.class.getName())
				.build());

		amb.addMeasure(k1Sum);
	}

	@Test
	public void testNoFx() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder().measure(mName).build());

		// List<Map<String, ?>> keySet =
		// output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		// Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of(mName, Set.of("Missing_FX_Rate-%s-USD-EUR".formatted(today))));
	}

	@Test
	public void testHasUnknownFromCcy() {
		prepareMeasures();

		rows.add(Map.of("color", "red", "ccyFrom", "unknownCcy", "k1", 234));

		ITabularView output = aqw.execute(AdhocQuery.builder().measure(mName).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of(mName,
								Set.of("Missing_FX_Rate-%s-unknownCcy-EUR".formatted(today),
										"Missing_FX_Rate-%s-USD-EUR".formatted(today))));
	}

	@Test
	public void testHasUnknownToCcy() {
		prepareMeasures();

		// Need a bit more than 1 USD for 1 EUR
		fxStorage.addFx(IForeignExchangeStorage.FXKey.builder().fromCcy("USD").toCcy("EUR").build(), 0.95D);

		ITabularView output = aqw.execute(AdhocQuery.builder().measure(mName).customMarker(Optional.of("XYZ")).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of(mName,
								Set.of("Missing_FX_Rate-%s-USD-XYZ".formatted(today),
										"Missing_FX_Rate-%s-EUR-XYZ".formatted(today))));
	}

	@Test
	public void testGrandTotalInJpy_customMarker() {
		prepareMeasures();

		// Need many JPY for one USD
		fxStorage.addFx(IForeignExchangeStorage.FXKey.builder().fromCcy("USD").toCcy("JPY").build(), 150D);
		// Need a bit more than 1 USD for 1 EUR
		fxStorage.addFx(IForeignExchangeStorage.FXKey.builder().fromCcy("USD").toCcy("EUR").build(), 0.95D);

		ITabularView output =
				aqw.execute(AdhocQuery.builder().measure(mName).customMarker(Optional.of("JPY")).explain(true).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(mName, 0D + 123 * 150D + 234 / 0.95D * 150D));
	}

	@Test
	public void testLackBucketor() {
		// We miss a bucketor aggregating the converted value per ccyFrom
		{
			amb.addMeasure(Bucketor.builder()
					.name(mName)
					.underlyings(Arrays.asList("k1"))
					.combinationKey(ForeignExchangeCombination.KEY)
					.build());

			amb.addMeasure(k1Sum);
		}

		Assertions.setMaxStackTraceElementsDisplayed(128);

		Assertions.assertThatThrownBy(() -> aqw.execute(AdhocQuery.builder().measure(mName).build()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasRootCauseInstanceOf(IllegalArgumentException.class)
				.hasStackTraceContaining("ccyFrom is not a sliced column");
	}

	@Test
	public void testExplain_grandTotal() {
		List<String> messages = AdhocExplainerTestHelper.listenForExplain(eventBus);

		prepareMeasures();

		// ITabularView output =
		aqw.execute(AdhocQuery.builder().measure(mName).customMarker(Optional.of("JPY")).explain(true).build());

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n"))).isEqualTo("""
				#0 m=k1.CCY(Bucketor) filter=matchAll groupBy=grandTotal customMarker=JPY
				\\-- #1 m=k1(Aggregator) filter=matchAll groupBy=(ccyFrom) customMarker=JPY
																								""".trim());

		Assertions.assertThat(messages).hasSize(2);
	}

	@Test
	public void testExplain_filter() {
		List<String> messages = AdhocExplainerTestHelper.listenForExplain(eventBus);

		prepareMeasures();

		// ITabularView output =
		aqw.execute(AdhocQuery.builder()
				.measure(mName)
				.customMarker(Optional.of("JPY"))
				.groupByAlso("letter")
				.andFilter("color", "red")
				.explain(true)
				.build());

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n"))).isEqualTo("""
				#0 m=k1.CCY(Bucketor) filter=color=red groupBy=(letter) customMarker=JPY
				\\-- #1 m=k1(Aggregator) filter=color=red groupBy=(ccyFrom, letter) customMarker=JPY
						""".trim());

		Assertions.assertThat(messages).hasSize(2);
	}
}
