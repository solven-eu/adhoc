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
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.ISliceFilter;
import lombok.extern.slf4j.Slf4j;

/**
 * This is useful to check advanced behaviors around customMarker. A legitimate case for customMarker is to force a
 * customMarker for a given measure, while other measure may be dynamic.
 *
 * This mostly duplicates {@link TestCubeQuery_Shiftor}, but requiring the shift to EUR to be provided as customMarker.
 */
@Slf4j
public class TestTransformator_Shiftor_contextValue extends ADagTest implements IAdhocTestConstants {

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("color", "red", "ccy", "EUR", "k1", 123));
		table().add(Map.of("color", "red", "ccy", "USD", "k1", 234));
		table().add(Map.of("color", "blue", "ccy", "EUR", "k1", 345));
		table().add(Map.of("color", "green", "ccy", "JPY", "k1", 456));
		// Lack measure: should not materialize coordinates on shift
		table().add(Map.of("color", "yellow", "ccy", "CHN"));
	}

	// Default is EUR
	String mName = "k1.dynamic";

	public static class ToCcyShifter implements IFilterEditor {

		@Override
		public ISliceFilter editFilter(FilterEditorContext filterEditorContext) {
			// For the sake of the unittest, we require the customMarker to be properly provided
			String ccy = Optional.ofNullable(filterEditorContext.getCustomMarker()).map(customMarker -> {
				String customMarkerAsString = customMarker.toString();
				if (customMarkerAsString.matches("[A-Z]{3}")) {
					return customMarkerAsString;
				} else {
					log.warn("Invalid customMarker: {}", customMarker);
					return "unknownCcy";
				}
			}).orElse("unknownCcy");
			return SimpleFilterEditor.shift(filterEditorContext.getFilter(), "ccy", ccy);
		}

		@Override
		public ISliceFilter editFilter(ISliceFilter input) {
			throw new UnsupportedOperationException("Where is FilterEditorContext?");
		}
	}

	void prepareMeasures() {
		forest.addMeasure(k1Sum);

		forest.addMeasure(Shiftor.builder()
				.name(mName)
				.underlying(k1Sum.getName())
				.editorKey(ToCcyShifter.class.getName())
				.build());
	}

	@Test
	public void testGrandTotal() {
		prepareMeasures();

		ITabularView output = cube().execute(CubeQuery.builder().measure(mName).customMarker("EUR").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat(coordinates).isEmpty();
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		});
	}

	@Test
	public void testGroupByCcy() {
		prepareMeasures();

		ITabularView output =
				cube().execute(CubeQuery.builder().measure(mName).groupByAlso("ccy").customMarker("EUR").build());

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

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(mName)
				.andFilter("ccy", "USD")
				.groupByAlso("ccy")
				.customMarker("EUR")
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("ccy", "USD");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		});
	}

	@Test
	public void testGroupByCcyFilterCcy_filteredCcy() {
		prepareMeasures();

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(mName)
				.andFilter("ccy", "EUR")
				.groupByAlso("ccy")
				.customMarker("EUR")
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("ccy", "EUR");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		});
	}

	@Test
	public void testUnknownCcy() {
		prepareMeasures();

		ITabularView output = cube()
				.execute(CubeQuery.builder().measure(mName).andFilter("ccy", "unknown").customMarker("EUR").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(0);
	}

	@Test
	public void testUnknownCcy_groupByColor() {
		prepareMeasures();

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(mName)
				.andFilter("ccy", "unknown")
				.groupByAlso("color")
				.customMarker("EUR")
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(0);
	}

	@Test
	public void testGroupByColor() {
		prepareMeasures();

		ITabularView output =
				cube().execute(CubeQuery.builder().measure(mName).groupByAlso("color").customMarker("EUR").build());

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
