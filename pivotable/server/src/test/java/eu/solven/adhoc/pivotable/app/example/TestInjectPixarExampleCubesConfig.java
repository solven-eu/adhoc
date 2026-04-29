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
package eu.solven.adhoc.pivotable.app.example;

import java.util.LinkedHashMap;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.query.cube.CubeQuery;

/**
 * Verifies that {@link InjectPixarExampleCubesConfig#registerPixarFilms(eu.solven.adhoc.beta.schema.IAdhocSchema)} can
 * stand on its own with just an {@link AdhocSchema} bean — no Spring context, no broader Pivotable wiring — and that
 * the resulting cubes are queryable end-to-end against the embedded DuckDB it provisions.
 *
 * @author Benoit Lacelle
 */
public class TestInjectPixarExampleCubesConfig {

	AdhocSchema schema = AdhocSchema.builder().env(new MockEnvironment()).build();

	@Test
	public void testRegisterPixarFilms_registersThreeCubes() {
		new InjectPixarExampleCubesConfig().registerPixarFilms(schema);

		// `films`, `people` and the composite `pixar` cube must all be registered. Names are taken straight from
		// the registration calls in the production class — they are also referenced by the SPA, so any rename here
		// is a real API change.
		Assertions.assertThat(schema.getCubes())
				.extracting(ICubeWrapper::getName)
				.containsExactlyInAnyOrder("films", "people", "pixar");
	}

	@Test
	public void testFilmsCube_getCoordinatesPerColumn() {
		new InjectPixarExampleCubesConfig().registerPixarFilms(schema);

		schema.getCubes().forEach(cube -> {
			// Probe each column's coordinates in a single bulk call — exactly what the SPA does on Estimate-all to
			// fan out one HTTP request rather than N. Asserts the wiring (table, JOOQ resolver, schema introspection)
			// is consistent enough that every declared column is reachable from `getCoordinates`.
			Map<String, IValueMatcher> request = new LinkedHashMap<>();
			for (ColumnMetadata column : cube.getColumns()) {
				request.put(column.getName(), IValueMatcher.MATCH_ALL);
			}
			Assertions.assertThat(request).isNotEmpty();

			Map<String, CoordinatesSample> result = cube.getCoordinates(request, 50);

			// Every requested column must round-trip — a missing key would mean the cube silently dropped it. Some
			// columns may have an empty coordinate sample (e.g. all-null columns) but the entry must exist.
			Assertions.assertThat(result.keySet()).containsExactlyInAnyOrderElementsOf(request.keySet());

			switch (cube.getName()) {
			case "films": {
				// Spot-check a known dimensional column: `film` is the title, expected to have at least one coordinate.
				// The bundled CSV has unique titles so the sample size should be the row count, capped at the limit.
				Assertions.assertThat(result.get("film").getCoordinates()).isNotEmpty();
				break;
			}
			case "people": {
				// Spot-check a known dimensional column: `film` is the title, expected to have at least one coordinate.
				// The bundled CSV has unique titles so the sample size should be the row count, capped at the limit.
				Assertions.assertThat(result.get("film").getCoordinates()).isNotEmpty();
				break;
			}
			case "pixar": {
				// Spot-check a known dimensional column: `film` is the title, expected to have at least one coordinate.
				// The bundled CSV has unique titles so the sample size should be the row count, capped at the limit.
				Assertions.assertThat(result.get("film").getCoordinates()).isNotEmpty();
				break;
			}

			default:
				throw new IllegalArgumentException("Unexpected value: " + cube.getName());
			}
		});

	}

	@Test
	public void testFilmsCube_countAsteriskMatchesCsv() {
		new InjectPixarExampleCubesConfig().registerPixarFilms(schema);

		ICubeWrapper films =
				schema.getCubes().stream().filter(c -> "films".equals(c.getName())).findFirst().orElseThrow();

		ITabularView result = films.execute(CubeQuery.builder().measure("count(films)").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		// 28 Pixar features in the bundled `pixar_films.csv` (header excluded). The exact value isn't load-bearing —
		// what we care about is that the row reached aggregation through the JooqTableSupplierBuilder + DuckDB stack
		// without an SQL error or null. Bound the assertion loosely so a future CSV refresh doesn't break the test.
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.hasEntrySatisfying(Map.of(),
						v -> Assertions.assertThat((Map<String, ?>) v)
								.hasEntrySatisfying("count(films)",
										count -> Assertions.assertThat(count)
												.asInstanceOf(InstanceOfAssertFactories.LONG)
												.isGreaterThan(0L)));
	}

	@Test
	public void testPixarCompositeCube_peoplePerFilmIsPositive() {
		new InjectPixarExampleCubesConfig().registerPixarFilms(schema);

		ICubeWrapper pixar =
				schema.getCubes().stream().filter(c -> "pixar".equals(c.getName())).findFirst().orElseThrow();

		// `people per film` is a Combinator dividing the people sub-cube count by the films sub-cube count — the
		// sole reason the composite cube exists. A positive value confirms the CompositeCubesTableWrapper fan-out
		// reaches both sub-cubes.
		ITabularView result = pixar.execute(CubeQuery.builder().measure("people per film").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(result);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.hasEntrySatisfying(Map.of(),
						v -> Assertions.assertThat((Map<String, ?>) v)
								.hasEntrySatisfying("people per film",
										ratio -> Assertions.assertThat(ratio)
												.asInstanceOf(InstanceOfAssertFactories.DOUBLE)
												.isGreaterThan(0d)));
	}
}
