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
import eu.solven.adhoc.ITabularView;
import eu.solven.adhoc.MapBasedTabularView;
import eu.solven.adhoc.aggregations.ICombination;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.aggregations.StandardOperatorsFactory;
import eu.solven.adhoc.aggregations.sum.SumElseSetAggregator;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.transformers.Bucketor;
import lombok.NonNull;

/**
 * This tests a typical usecase: given financial data for different currencies, we need to convert the underlying data
 * into a common currency, by a multiplication with the proper rate, depending on current currency. Each data are
 * aggregated by their own currency, then converted into a common currency, and finally aggregated.
 */
public class TestAdhocQueryFx extends ADagTest implements IAdhocTestConstants {

	ForeignExchangeStorage fxStorage = new ForeignExchangeStorage();

	LocalDate today = LocalDate.now();

	public final AdhocQueryEngine aqe = AdhocQueryEngine.builder()
			.eventBus(eventBus)
			.measureBag(amb)
			.operatorsFactory(makeOperatorsFactory(fxStorage))
			.build();

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
	public void feedDb() {
		rows.add(Map.of("l", "A", "ccyFrom", "USD", "k1", 123));
		rows.add(Map.of("l", "A", "ccyFrom", "EUR", "k1", 234));
	}

	void prepareMeasures() {
		amb.addMeasure(Bucketor.builder()
				.name("k1.USD")
				.underlyings(Arrays.asList("k1"))
				.groupBy(GroupByColumns.named("ccyFrom"))
				.combinationKey(ForeignExchangeCombination.KEY)
				// This tests we can refer an aggregation key but its class
				.aggregationKey(SumElseSetAggregator.class.getName())
				.build());

		amb.addMeasure(k1Sum);
	}

	@Test
	public void testNoFx() {
		prepareMeasures();

		ITabularView output = aqe.execute(AdhocQuery.builder().measure("k1.USD").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("k1.USD", Set.of("Missing_FX_Rate-%s-EUR-USD".formatted(today))));
	}

	@Test
	public void testHasUnknownFromCcy() {
		prepareMeasures();

		rows.add(Map.of("l", "A", "ccyFrom", "unknownCcy", "k1", 234));

		ITabularView output = aqe.execute(AdhocQuery.builder().measure("k1.USD").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("k1.USD",
								Set.of("Missing_FX_Rate-%s-unknownCcy-USD".formatted(today),
										"Missing_FX_Rate-%s-EUR-USD".formatted(today))));
	}

	@Test
	public void testHasUnknownToCcy() {
		prepareMeasures();

		// Need a bit more than 1 USD for 1 EUR
		fxStorage.addFx(IForeignExchangeStorage.FXKey.builder().fromCcy("USD").toCcy("EUR").build(), 0.95D);

		ITabularView output =
				aqe.execute(AdhocQuery.builder().measure("k1.USD").customMarker(Optional.of("XYZ")).build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("k1.USD",
								Set.of("Missing_FX_Rate-%s-USD-XYZ".formatted(today),
										"Missing_FX_Rate-%s-EUR-XYZ".formatted(today))));
	}

	@Test
	public void testGrandTotalInJpy() {
		prepareMeasures();

		// Need many JPY for one USD
		fxStorage.addFx(IForeignExchangeStorage.FXKey.builder().fromCcy("USD").toCcy("JPY").build(), 150D);
		// Need a bit more than 1 USD for 1 EUR
		fxStorage.addFx(IForeignExchangeStorage.FXKey.builder().fromCcy("USD").toCcy("EUR").build(), 0.95D);

		ITabularView output =
				aqe.execute(AdhocQuery.builder().measure("k1.USD").customMarker(Optional.of("JPY")).debug(true).build(),
						rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("k1.USD", 0D + 123 * 150D + 234 / 0.95D * 150D));
	}

	@Test
	public void testLackBucketor() {
		// We miss a bucketor aggregating the converted value per ccyFrom
		{
			amb.addMeasure(Bucketor.builder()
					.name("k1.USD")
					.underlyings(Arrays.asList("k1"))
					.combinationKey(ForeignExchangeCombination.KEY)
					.build());

			amb.addMeasure(k1Sum);
		}

		Assertions.assertThatThrownBy(() -> aqe.execute(AdhocQuery.builder().measure("k1.USD").build(), rows))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
