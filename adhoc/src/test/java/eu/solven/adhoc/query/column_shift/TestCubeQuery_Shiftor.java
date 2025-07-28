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
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.filter.editor.IFilterEditor;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.measure.IMeasureForestVisitor;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.Shiftor;
import eu.solven.adhoc.measure.sum.AvgAggregation;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import lombok.extern.slf4j.Slf4j;

/**
 * Standard behavior of {@link Shiftor}.
 */
@Slf4j
public class TestCubeQuery_Shiftor extends ADagTest implements IAdhocTestConstants {

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
	String mName = "k1.EUR";

	public static class ToEurShifter implements IFilterEditor {
		@Override
		public ISliceFilter editFilter(ISliceFilter input) {
			return SimpleFilterEditor.shift(input, "ccy", "EUR");
		}
	}

	@BeforeEach
	void prepareMeasures() {
		forest.addMeasure(k1Sum);

		forest.addMeasure(Shiftor.builder()
				.name(mName)
				.underlying(k1Sum.getName())
				.editorKey(ToEurShifter.class.getName())
				.build());
	}

	@Test
	public void testGrandTotal() {
		ITabularView output = cube().execute(CubeQuery.builder().measure(mName).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat(coordinates).isEmpty();
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		});
	}

	@Test
	public void testGroupByCcy() {
		ITabularView output = cube().execute(CubeQuery.builder().measure(mName).groupByAlso("ccy").build());

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
		ITabularView output =
				cube().execute(CubeQuery.builder().measure(mName).andFilter("ccy", "USD").groupByAlso("ccy").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("ccy", "USD");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		});
	}

	@Test
	public void testGroupByCcyFilterCcy_filteredCcy() {
		ITabularView output =
				cube().execute(CubeQuery.builder().measure(mName).andFilter("ccy", "EUR").groupByAlso("ccy").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("ccy", "EUR");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123 + 345);
		});
	}

	@Test
	public void testUnknownCcy() {
		ITabularView output = cube().execute(CubeQuery.builder().measure(mName).andFilter("ccy", "unknown").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(0);
	}

	@Test
	public void testUnknownCcy_groupByColor() {
		ITabularView output = cube()
				.execute(CubeQuery.builder().measure(mName).andFilter("ccy", "unknown").groupByAlso("color").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(0);
	}

	@Test
	public void testGroupByColor() {
		ITabularView output = cube().execute(CubeQuery.builder().measure(mName).groupByAlso("color").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(2).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("color", "red");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123);
		}).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("color", "blue");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 345);
		});
	}

	// This test is interesting to ensure Shiftor does not mix-up filters from slice and filters from queryStep
	@Test
	public void testFilterByComplexColor() {
		ITabularView output = cube()
				.execute(CubeQuery.builder().measure(mName).andFilter(ColumnFilter.isLike("color", "bl%")).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).isEmpty();
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 345);
		});
	}

	// Shiftor over an IHasCarrier may lead to issues. Most of them are not specific to Shiftor.
	@Test
	public void testGroupByColor_avg() {
		toAvg();
		ITabularView output = cube().execute(CubeQuery.builder().measure(mName).groupByAlso("color").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(2).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("color", "red");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 123);
		}).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat((Map) coordinates).hasSize(1).containsEntry("color", "blue");
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + 345);
		});
	}

	@Test
	public void testGrandTotal_avg() {
		toAvg();
		ITabularView output = cube().execute(CubeQuery.builder().measure(mName).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).anySatisfy((coordinates, measures) -> {
			Assertions.assertThat(coordinates).isEmpty();
			Assertions.assertThat((Map) measures).hasSize(1).containsEntry(mName, 0L + (123 + 345) / 2);
		});
	}

	private void toAvg() {
		forest.acceptVisitor(new IMeasureForestVisitor() {
			@Override
			public Set<IMeasure> mapMeasure(IMeasure measure) {
				if (measure instanceof Aggregator aggregator) {
					return Set.of(aggregator.toBuilder().aggregationKey(AvgAggregation.KEY).build());
				} else {
					return Set.of(measure);
				}
			}
		});
	}

}
