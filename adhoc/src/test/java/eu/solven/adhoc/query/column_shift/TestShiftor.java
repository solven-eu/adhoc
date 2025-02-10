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
package eu.solven.adhoc.query.column_shift;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.step.Shiftor;
import eu.solven.adhoc.measure.step.Shiftor.ISliceEditor;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.foreignexchange.ForeignExchangeCombination;
import eu.solven.adhoc.query.foreignexchange.IForeignExchangeStorage;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * This is useful to check advanced behaviors around customMarker. A legitimate case for customMarker is to force a
 * customMarker for a given measure, while other measure may be dynamic.
 */
@Slf4j
public class TestShiftor extends ADagTest implements IAdhocTestConstants {

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
		rows.add(Map.of("color", "red", "ccy", "EUR", "k1", 123));
		rows.add(Map.of("color", "red", "ccy", "USD", "k1", 234));
		rows.add(Map.of("color", "blue", "ccy", "EUR", "k1", 345));
		rows.add(Map.of("color", "green", "ccy", "JPY", "k1", 456));
		// Lack measure: should not materialize coordinates on shift
		rows.add(Map.of("color", "yellow", "ccy", "CHN"));
	}

	// Default is EUR
	String mName = "k1.EUR";

	void prepareMeasures() {
		amb.addMeasure(k1Sum);

		ISliceEditor toEur = Shiftor.shift("ccy", "EUR");
		amb.addMeasure(Shiftor.builder().name(mName).underlying(k1Sum.getName()).sliceEditor(toEur).build());
	}

	@Test
	public void testGrandTotal() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder().measure(mName).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat(coordinates).isEmpty();
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		});
	}

	@Test
	public void testGroupByCcy() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder().measure(mName).groupByAlso("ccy").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(3).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("ccy", "EUR");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		}).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("ccy", "USD");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		}).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("ccy", "JPY");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		});
	}

	@Test
	public void testGroupByCcyFilterCcy_notFiltered() {
		prepareMeasures();

		ITabularView output =
				aqw.execute(AdhocQuery.builder().measure(mName).andFilter("ccy", "USD").groupByAlso("ccy").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("ccy", "USD");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		});
	}

	@Test
	public void testGroupByCcyFilterCcy_filteredCcy() {
		prepareMeasures();

		ITabularView output =
				aqw.execute(AdhocQuery.builder().measure(mName).andFilter("ccy", "EUR").groupByAlso("ccy").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("ccy", "EUR");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		});
	}

	@Test
	public void testUnknownCcy() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder().measure(mName).andFilter("ccy", "unknown").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(0)
		// .anySatisfy((coordinates, measures) -> {
		// Assertions.assertThat(coordinates).isEmpty();
		// Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		// })
		;
	}

	@Test
	public void testUnknownCcy_groupByColor() {
		prepareMeasures();

		ITabularView output = aqw
				.execute(AdhocQuery.builder().measure(mName).andFilter("ccy", "unknown").groupByAlso("color").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(0)
		// .anySatisfy((coordinates, measures) -> {
		// Assertions.assertThat(coordinates).isEmpty();
		// Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		// })
		;
	}

	@Test
	public void testGroupByColor() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder().measure(mName).groupByAlso("color").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(2).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("color", "red");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123);
		}).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("color", "blue");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 345);
		});
	}
}
