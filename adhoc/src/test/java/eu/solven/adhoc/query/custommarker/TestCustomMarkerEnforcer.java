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
package eu.solven.adhoc.query.custommarker;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
import eu.solven.adhoc.aggregations.ICombination;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.aggregations.StandardOperatorsFactory;
import eu.solven.adhoc.aggregations.sum.SumElseSetAggregator;
import eu.solven.adhoc.dag.AdhocCubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.foreignexchange.ForeignExchangeCombination;
import eu.solven.adhoc.query.foreignexchange.ForeignExchangeStorage;
import eu.solven.adhoc.query.foreignexchange.IForeignExchangeStorage;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.transformers.Bucketor;
import eu.solven.adhoc.view.ITabularView;
import eu.solven.adhoc.view.MapBasedTabularView;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * This is useful to check advanced behaviors around customMarker. A legitimate case for customMarker is to force a
 * customMarker for a given measure, while other measure may be dynamic.
 */
@Slf4j
public class TestCustomMarkerEnforcer extends ADagTest implements IAdhocTestConstants {

	ForeignExchangeStorage fxStorage = new ForeignExchangeStorage();

	LocalDate today = LocalDate.now();

	public final AdhocQueryEngine aqe = editEngine().operatorsFactory(makeOperatorsFactory(fxStorage)).build();
	public final AdhocCubeWrapper aqw = AdhocCubeWrapper.builder().table(rows).engine(aqe).measures(amb).build();

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

	// Default is EUR
	String mName = "k1.CCY";
	String mNameEUR = "k1.EUR";
	String mNameUSD = "k1.USD";

	void prepareMeasures() {
		amb.addMeasure(Bucketor.builder()
				.name(mName)
				.underlyings(Arrays.asList(k1Sum.getName()))
				.groupBy(GroupByColumns.named("ccyFrom"))
				.combinationKey(ForeignExchangeCombination.KEY)
				// This tests we can refer an aggregation key but its class
				.aggregationKey(SumElseSetAggregator.class.getName())
				.build());

		amb.addMeasure(
				CustomMarkerEditor.builder().name(mNameEUR).underlying(mName).customMarkerEditor(optCustomMarker -> {
					return forceCcy("EUR", optCustomMarker);
				}).build());

		amb.addMeasure(
				CustomMarkerEditor.builder().name(mNameUSD).underlying(mName).customMarkerEditor(optCustomMarker -> {
					return forceCcy("USD", optCustomMarker);
				}).build());

		amb.addMeasure(k1Sum);
	}

	private static Optional<String> forceCcy(String ccy, Optional<?> optCustomMarker) {
		if (optCustomMarker.isEmpty()) {
			return Optional.of(ccy);
		} else {
			Object currentCustom = optCustomMarker.get();

			if (currentCustom instanceof String asString) {
				log.debug("Enforcing from custom={} to {}", asString, ccy);
				return Optional.of(ccy);
			} else {
				throw new UnsupportedOperationException(
						"Not managed: %s".formatted(PepperLogHelper.getObjectAndClass(currentCustom)));
			}
		}
	}

	private Map<String, Object> roundDoubles(Map<String, ?> measures) {
		Map<String, Object> rounded = new HashMap<>();

		measures.forEach((k, v) -> {
			Object roundedV = roundDouble(v);

			rounded.put(k, roundedV);
		});

		return rounded;
	}

	private static Object roundDouble(Object v) {
		Object roundedV;

		if (v instanceof Float || v instanceof Double) {
			roundedV = BigDecimal.valueOf(((Number) v).doubleValue()).setScale(3, RoundingMode.CEILING).doubleValue();
		} else {
			roundedV = v;
		}
		return roundedV;
	}

	@Test
	public void testNoFx() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder().measure(mName, mNameEUR, mNameUSD).build());

		List<Map<String, ?>> keySet = output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat(coordinates).isEmpty();
			Assertions.assertThat((Map) measures)
					.hasSize(3)
					.containsEntry(mName, Set.of("Missing_FX_Rate-%s-USD-EUR".formatted(today)));
		});
	}

	@Test
	public void testHasFX() {
		prepareMeasures();

		// Need a bit more than 1 USD for 1 EUR
		fxStorage.addFx(IForeignExchangeStorage.FXKey.builder().fromCcy("USD").toCcy("EUR").build(), 0.95D);

		ITabularView output = aqw.execute(AdhocQuery.builder().measure(mName, mNameEUR, mNameUSD).build());

		List<Map<String, ?>> keySet = output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat(coordinates).isEmpty();
			Assertions.assertThat(roundDoubles(measures))
					.hasSize(3)
					.containsEntry(mName, roundDouble(0D + 123 * 0.95D + 234))
					.containsEntry(mNameEUR, roundDouble(0D + 123 * 0.95D + 234))
					.containsEntry(mNameUSD, roundDouble(0D + 123 + 234 / 0.95D));
		});
	}

	@Test
	public void testGrandTotalInJpy_customMarker() {
		prepareMeasures();

		// Need many JPY for one USD
		fxStorage.addFx(IForeignExchangeStorage.FXKey.builder().fromCcy("USD").toCcy("JPY").build(), 150D);
		// Need a bit more than 1 USD for 1 EUR
		fxStorage.addFx(IForeignExchangeStorage.FXKey.builder().fromCcy("USD").toCcy("EUR").build(), 0.95D);

		ITabularView output = aqw.execute(AdhocQuery.builder()
				.measure(mName, mNameEUR, mNameUSD)
				.customMarker(Optional.of("JPY"))
				.debug(true)
				.build());

		List<Map<String, ?>> keySet = output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat(coordinates).isEmpty();
			Assertions.assertThat(roundDoubles(measures))
					.hasSize(3)
					.containsEntry(mName, roundDouble(0D + 123 * 150D + 234 / 0.95D * 150D))
					.containsEntry(mNameEUR, roundDouble(0D + 123 * 0.95D + 234))
					.containsEntry(mNameUSD, roundDouble(0D + 123 + 234 / 0.95D));
		});
	}
}
