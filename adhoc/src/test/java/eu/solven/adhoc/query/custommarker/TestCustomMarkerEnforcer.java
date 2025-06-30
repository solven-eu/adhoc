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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.measure.sum.SumElseSetAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.foreignexchange.ForeignExchangeCombination;
import eu.solven.adhoc.query.foreignexchange.ForeignExchangeStorage;
import eu.solven.adhoc.query.foreignexchange.IForeignExchangeStorage;
import eu.solven.adhoc.query.groupby.GroupByColumns;
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

	@Override
	public AdhocFactories makeFactories() {
		return super.makeFactories().toBuilder().operatorFactory(makeOperatorsFactory(fxStorage)).build();
	}

	private @NonNull IOperatorFactory makeOperatorsFactory(IForeignExchangeStorage fxStorage) {

		return new StandardOperatorFactory() {
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
		table().add(Map.of("l", "A", "ccyFrom", "USD", "k1", 123));
		table().add(Map.of("l", "A", "ccyFrom", "EUR", "k1", 234));
	}

	// Default is EUR
	String mName = "k1.CCY";
	String mNameEUR = "k1.EUR";
	String mNameUSD = "k1.USD";

	void prepareMeasures() {
		forest.addMeasure(Partitionor.builder()
				.name(mName)
				.underlyings(Arrays.asList(k1Sum.getName()))
				.groupBy(GroupByColumns.named("ccyFrom"))
				.combinationKey(ForeignExchangeCombination.KEY)
				.aggregationKey(SumElseSetAggregation.class.getName())
				.build());

		forest.addMeasure(
				CustomMarkerEditor.builder().name(mNameEUR).underlying(mName).customMarkerEditor(optCustomMarker -> {
					return forceCcy("EUR", optCustomMarker);
				}).build());

		forest.addMeasure(
				CustomMarkerEditor.builder().name(mNameUSD).underlying(mName).customMarkerEditor(optCustomMarker -> {
					return forceCcy("USD", optCustomMarker);
				}).build());

		forest.addMeasure(k1Sum);
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

		ITabularView output = cube().execute(CubeQuery.builder().measure(mName, mNameEUR, mNameUSD).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat(coordinates).isEmpty();
			Assertions.assertThat((Map) measures)
					.hasSize(3)
					.hasEntrySatisfying(mName,
							l -> Assertions.assertThat((Set) l).contains("Missing_FX_Rate-%s-USD-EUR".formatted(today)))
					.hasEntrySatisfying(mNameEUR,
							l -> Assertions.assertThat((Set) l).contains("Missing_FX_Rate-%s-USD-EUR".formatted(today)))
					.hasEntrySatisfying(mNameUSD,
							l -> Assertions.assertThat((Set) l)
									.contains("Missing_FX_Rate-%s-EUR-USD".formatted(today)));
		});
	}

	@Test
	public void testHasFX() {
		prepareMeasures();

		// Need a bit more than 1 USD for 1 EUR
		fxStorage.addFx(IForeignExchangeStorage.FXKey.builder().fromCcy("USD").toCcy("EUR").build(), 0.95D);

		ITabularView output = cube().execute(CubeQuery.builder().measure(mName, mNameEUR, mNameUSD).build());

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

		ITabularView output = cube().execute(
				CubeQuery.builder().measure(mName, mNameEUR, mNameUSD).customMarker(Optional.of("JPY")).build());

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
