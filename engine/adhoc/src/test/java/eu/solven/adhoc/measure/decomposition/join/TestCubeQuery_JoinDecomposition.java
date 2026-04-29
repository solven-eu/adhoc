/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure.decomposition.join;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * End-to-end tests for {@link JoinDecomposition} with <em>multiple</em> output columns. The scenario: table rows carry
 * {@code (asOfDate, country, k1)}; a lookup table maps {@code (asOfDate, country)} → {@code (weather,
 * language)}. A {@link Dispatchor} wired with the {@link JoinDecomposition} enriches each row with both columns.
 */
public class TestCubeQuery_JoinDecomposition extends ADagTest implements IAdhocTestConstants {

	InMemoryJoinDefinition joinDef = new InMemoryJoinDefinition();

	@Override
	public AdhocFactories makeFactories() {
		return super.makeFactories().toBuilder().operatorFactory(makeOperatorFactory()).build();
	}

	protected IOperatorFactory makeOperatorFactory() {
		return new StandardOperatorFactory() {
			@Override
			public IDecomposition makeDecomposition(String key, Map<String, ?> options) {
				if (JoinDecomposition.KEY.equals(key)) {
					return new JoinDecomposition(options, joinDef);
				}
				return super.makeDecomposition(key, options);
			}
		};
	}

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("asOfDate", "2026-04-17", "country", "FR", "k1", 10));
		table().add(Map.of("asOfDate", "2026-04-17", "country", "DE", "k1", 20));
		table().add(Map.of("asOfDate", "2026-04-18", "country", "FR", "k1", 30));

		// Lookup: (asOfDate, country) → (weather, language)
		joinDef.put(Map.of("asOfDate", "2026-04-17", "country", "FR"),
				Map.of("weather", "sunny", "language", "French"));
		joinDef.put(Map.of("asOfDate", "2026-04-17", "country", "DE"),
				Map.of("weather", "rainy", "language", "German"));
		joinDef.put(Map.of("asOfDate", "2026-04-18", "country", "FR"),
				Map.of("weather", "cloudy", "language", "French"));
	}

	private void prepareMeasures() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(Dispatchor.builder()
				.name("k1.enriched")
				.underlying(k1Sum.getName())
				.decompositionKey(JoinDecomposition.KEY)
				.decompositionOption(JoinDecomposition.K_INPUTS, List.of("asOfDate", "country"))
				.decompositionOption(JoinDecomposition.K_OUTPUTS, List.of("weather", "language"))
				.build());
	}

	// ---- GROUP BY single output column ---------------------------------------------------------------------

	@Test
	public void testGroupByWeather() {
		prepareMeasures();

		ITabularView view = cube()
				.execute(CubeQuery.builder().measure("k1.enriched").groupBy(GroupByColumns.named("weather")).build());
		MapBasedTabularView map = MapBasedTabularView.load(view);

		Assertions.assertThat(map.getCoordinatesToValues())
				.containsEntry(Map.of("weather", "sunny"), Map.of("k1.enriched", 10L))
				.containsEntry(Map.of("weather", "rainy"), Map.of("k1.enriched", 20L))
				.containsEntry(Map.of("weather", "cloudy"), Map.of("k1.enriched", 30L))
				.hasSize(3);
	}

	@Test
	public void testGroupByLanguage() {
		prepareMeasures();

		ITabularView view = cube()
				.execute(CubeQuery.builder().measure("k1.enriched").groupBy(GroupByColumns.named("language")).build());
		MapBasedTabularView map = MapBasedTabularView.load(view);

		// French: 10 + 30 = 40, German: 20
		Assertions.assertThat(map.getCoordinatesToValues())
				.containsEntry(Map.of("language", "French"), Map.of("k1.enriched", 40L))
				.containsEntry(Map.of("language", "German"), Map.of("k1.enriched", 20L))
				.hasSize(2);
	}

	// ---- GROUP BY multiple output columns ------------------------------------------------------------------

	@Test
	public void testGroupByWeatherAndLanguage() {
		prepareMeasures();

		ITabularView view = cube().execute(CubeQuery.builder()
				.measure("k1.enriched")
				.groupBy(GroupByColumns.named("weather", "language"))
				.build());
		MapBasedTabularView map = MapBasedTabularView.load(view);

		Assertions.assertThat(map.getCoordinatesToValues())
				.containsEntry(Map.of("weather", "sunny", "language", "French"), Map.of("k1.enriched", 10L))
				.containsEntry(Map.of("weather", "rainy", "language", "German"), Map.of("k1.enriched", 20L))
				.containsEntry(Map.of("weather", "cloudy", "language", "French"), Map.of("k1.enriched", 30L))
				.hasSize(3);
	}

	@Test
	public void testGroupByCountryAndWeather() {
		prepareMeasures();

		ITabularView view = cube().execute(
				CubeQuery.builder().measure("k1.enriched").groupBy(GroupByColumns.named("country", "weather")).build());
		MapBasedTabularView map = MapBasedTabularView.load(view);

		Assertions.assertThat(map.getCoordinatesToValues())
				.containsEntry(Map.of("country", "FR", "weather", "sunny"), Map.of("k1.enriched", 10L))
				.containsEntry(Map.of("country", "DE", "weather", "rainy"), Map.of("k1.enriched", 20L))
				.containsEntry(Map.of("country", "FR", "weather", "cloudy"), Map.of("k1.enriched", 30L))
				.hasSize(3);
	}

	// ---- Grand total / GROUP BY without output columns -----------------------------------------------------

	@Test
	public void testGrandTotal() {
		prepareMeasures();

		ITabularView view = cube().execute(CubeQuery.builder().measure("k1.enriched").build());
		MapBasedTabularView map = MapBasedTabularView.load(view);

		Assertions.assertThat(map.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of("k1.enriched", 60L))
				.hasSize(1);
	}

	@Test
	public void testGroupByCountry_noOutputColumn() {
		prepareMeasures();

		ITabularView view = cube()
				.execute(CubeQuery.builder().measure("k1.enriched").groupBy(GroupByColumns.named("country")).build());
		MapBasedTabularView map = MapBasedTabularView.load(view);

		Assertions.assertThat(map.getCoordinatesToValues())
				.containsEntry(Map.of("country", "FR"), Map.of("k1.enriched", 40L))
				.containsEntry(Map.of("country", "DE"), Map.of("k1.enriched", 20L))
				.hasSize(2);
	}

	// ---- Filters on output columns -------------------------------------------------------------------------

	@Test
	public void testFilterWeatherSunny_grandTotal() {
		prepareMeasures();

		ITabularView view =
				cube().execute(CubeQuery.builder().measure("k1.enriched").andFilter("weather", "sunny").build());
		MapBasedTabularView map = MapBasedTabularView.load(view);

		Assertions.assertThat(map.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of("k1.enriched", 10L))
				.hasSize(1);
	}

	@Test
	public void testFilterWeatherSunny_groupByCountry() {
		prepareMeasures();

		ITabularView view = cube().execute(CubeQuery.builder()
				.measure("k1.enriched")
				.groupBy(GroupByColumns.named("country"))
				.andFilter("weather", "sunny")
				.build());
		MapBasedTabularView map = MapBasedTabularView.load(view);

		Assertions.assertThat(map.getCoordinatesToValues())
				.containsEntry(Map.of("country", "FR"), Map.of("k1.enriched", 10L))
				.hasSize(1);
	}

	@Test
	public void testFilterLanguageFrench_groupByWeather() {
		prepareMeasures();

		ITabularView view = cube().execute(CubeQuery.builder()
				.measure("k1.enriched")
				.groupBy(GroupByColumns.named("weather"))
				.andFilter("language", "French")
				.build());
		MapBasedTabularView map = MapBasedTabularView.load(view);

		// French rows: sunny (10), cloudy (30)
		Assertions.assertThat(map.getCoordinatesToValues())
				.containsEntry(Map.of("weather", "sunny"), Map.of("k1.enriched", 10L))
				.containsEntry(Map.of("weather", "cloudy"), Map.of("k1.enriched", 30L))
				.hasSize(2);
	}

	// ---- Mixed: filter on native column, GROUP BY output column --------------------------------------------

	@Test
	public void testGroupByWeather_filterCountryFR() {
		prepareMeasures();

		ITabularView view = cube().execute(CubeQuery.builder()
				.measure("k1.enriched")
				.groupBy(GroupByColumns.named("weather"))
				.andFilter("country", "FR")
				.build());
		MapBasedTabularView map = MapBasedTabularView.load(view);

		Assertions.assertThat(map.getCoordinatesToValues())
				.containsEntry(Map.of("weather", "sunny"), Map.of("k1.enriched", 10L))
				.containsEntry(Map.of("weather", "cloudy"), Map.of("k1.enriched", 30L))
				.hasSize(2);
	}

	@Test
	public void testGroupByLanguage_filterCountryFR() {
		prepareMeasures();

		ITabularView view = cube().execute(CubeQuery.builder()
				.measure("k1.enriched")
				.groupBy(GroupByColumns.named("language"))
				.andFilter("country", "FR")
				.build());
		MapBasedTabularView map = MapBasedTabularView.load(view);

		// Only French rows for FR
		Assertions.assertThat(map.getCoordinatesToValues())
				.containsEntry(Map.of("language", "French"), Map.of("k1.enriched", 40L))
				.hasSize(1);
	}
}
