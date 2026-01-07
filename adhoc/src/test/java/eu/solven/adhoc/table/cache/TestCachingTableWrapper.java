/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.table.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.CubeQuery.CubeQueryBuilder;
import eu.solven.adhoc.util.AdhocBenchmark;
import eu.solven.adhoc.util.TestAdhocIntegrationTests;

public class TestCachingTableWrapper extends ADagTest implements IAdhocTestConstants {

	CachingTableWrapper caching = CachingTableWrapper.builder().decorated(tableSupplier.get()).build();
	ICubeWrapper cachingCube =
			CubeWrapper.builder().table(caching).engine(engine()).forest(forest).eventBus(eventBus()).build();

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("c", "c1", "d", "d1", "k1", 123));
		table().add(Map.of("c", "c2", "d", "d2", "k1", 234, "k2", 345));

		forest.addMeasure(Aggregator.countAsterisk());
		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);
	}

	@Test
	public void testChangeMaximumWeight() {
		CachingTableWrapper.defaultCacheBuilder().maximumWeight(123).build();
	}

	@Test
	public void testCache_emptyMeasure() {
		ITabularView firstView = cachingCube.execute(CubeQuery.builder().build());
		Assertions.assertThat(MapBasedTabularView.load(firstView).getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of())
				.hasSize(1);
		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(0);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(1);
		});

		ITabularView secondView = cachingCube.execute(CubeQuery.builder().build());
		Assertions.assertThat(MapBasedTabularView.load(secondView).getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of())
				.hasSize(1);
		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(1);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(1);
		});
	}

	@Test
	public void testCache_count_sameRequest() {
		String m = Aggregator.countAsterisk().getName();
		ITabularView firstView = cachingCube.execute(CubeQuery.builder().measure(m).build());
		Assertions.assertThat(MapBasedTabularView.load(firstView).getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(m, 0L + 2))
				.hasSize(1);
		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(0);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(1);
		});

		ITabularView secondView = cachingCube.execute(CubeQuery.builder().measure(m).build());
		Assertions.assertThat(MapBasedTabularView.load(secondView).getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(m, 0L + 2))
				.hasSize(1);

		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(1);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(1);
		});
	}

	@Test
	public void testCache_count_sameRequest_noCache() {
		String m = Aggregator.countAsterisk().getName();
		ITabularView firstView =
				cachingCube.execute(CubeQuery.builder().measure(m).option(StandardQueryOptions.NO_CACHE).build());
		Assertions.assertThat(MapBasedTabularView.load(firstView).getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(m, 0L + 2))
				.hasSize(1);
		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(0);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(0);
		});
	}

	@Test
	public void testCache_count_differentGroupBy() {
		String m = Aggregator.countAsterisk().getName();
		ITabularView firstView = cachingCube.execute(CubeQuery.builder().measure(m).build());
		Assertions.assertThat(MapBasedTabularView.load(firstView).getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(m, 0L + 2))
				.hasSize(1);
		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(0);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(1);
		});

		ITabularView secondView = cachingCube.execute(CubeQuery.builder().measure(m).groupByAlso("c").build());
		Assertions.assertThat(MapBasedTabularView.load(secondView).getCoordinatesToValues())
				.containsEntry(Map.of("c", "c1"), Map.of(m, 0L + 1))
				.containsEntry(Map.of("c", "c2"), Map.of(m, 0L + 1))
				.hasSize(2);

		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(0);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(2);
		});
	}

	@Test
	public void testCache_count_differentFilter() {
		String m = Aggregator.countAsterisk().getName();
		ITabularView firstView =
				cachingCube.execute(CubeQuery.builder().measure(m).groupByAlso("c").andFilter("d", "d1").build());
		Assertions.assertThat(MapBasedTabularView.load(firstView).getCoordinatesToValues())
				.containsEntry(Map.of("c", "c1"), Map.of(m, 0L + 1))
				.hasSize(1);
		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(0);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(1);
		});

		ITabularView secondView =
				cachingCube.execute(CubeQuery.builder().measure(m).groupByAlso("c").andFilter("d", "d2").build());
		Assertions.assertThat(MapBasedTabularView.load(secondView).getCoordinatesToValues())
				.containsEntry(Map.of("c", "c2"), Map.of(m, 0L + 1))
				.hasSize(1);

		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(0);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(2);
		});
	}

	@Test
	public void testCache_count_differentCustomMarkers() {
		String m = Aggregator.countAsterisk().getName();
		ITabularView firstView = cachingCube.execute(
				CubeQuery.builder().measure(m).groupByAlso("c").andFilter("d", "d1").customMarker("EUR").build());
		Assertions.assertThat(MapBasedTabularView.load(firstView).getCoordinatesToValues())
				.containsEntry(Map.of("c", "c1"), Map.of(m, 0L + 1))
				.hasSize(1);
		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(0);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(1);
		});

		ITabularView secondView = cachingCube.execute(
				CubeQuery.builder().measure(m).groupByAlso("c").andFilter("d", "d1").customMarker("USD").build());
		Assertions.assertThat(MapBasedTabularView.load(secondView).getCoordinatesToValues())
				.containsEntry(Map.of("c", "c1"), Map.of(m, 0L + 1))
				.hasSize(1);

		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(1);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(1);
		});
	}

	@Test
	public void testCache_additionalMeasure() {
		String m1 = Aggregator.countAsterisk().getName();

		ITabularView firstView = cachingCube.execute(CubeQuery.builder().measure(m1, k1Sum.getName()).build());
		Assertions.assertThat(MapBasedTabularView.load(firstView).getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(m1, 0L + 2, k1Sum.getName(), 0L + 123 + 234))
				.hasSize(1);
		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(0);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(2);
		});

		ITabularView secondView = cachingCube.execute(CubeQuery.builder().measure(m1).build());
		Assertions.assertThat(MapBasedTabularView.load(secondView).getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(m1, 0L + 2))
				.hasSize(1);

		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(1);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(2);
		});
	}

	@Test
	public void testCache_additionalMeasure_bothThenOnlyOne() {
		ITabularView firstView =
				cachingCube.execute(CubeQuery.builder().measure(k1Sum.getName(), k2Sum.getName()).build());
		Assertions.assertThat(MapBasedTabularView.load(firstView).getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123 + 234, k2Sum.getName(), 0L + 345))
				.hasSize(1);
		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(0);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(2);
		});

		ITabularView secondView = cachingCube.execute(CubeQuery.builder().measure(k1Sum.getName()).build());
		Assertions.assertThat(MapBasedTabularView.load(secondView).getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1Sum.getName(), 0L + 123 + 234))
				.hasSize(1);

		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(1);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(2);
		});
	}

	@Test
	public void testCache_additionalMeasure_measureWithMoreSlices_thenAddMeasureWithLessSlices() {
		ITabularView firstView =
				cachingCube.execute(CubeQuery.builder().groupByAlso("c").measure(k1Sum.getName()).build());
		Assertions.assertThat(MapBasedTabularView.load(firstView).getCoordinatesToValues())
				.containsEntry(Map.of("c", "c1"), Map.of(k1Sum.getName(), 0L + 123))
				.containsEntry(Map.of("c", "c2"), Map.of(k1Sum.getName(), 0L + 234))
				.hasSize(2);
		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(0);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(1);
		});

		ITabularView secondView = cachingCube
				.execute(CubeQuery.builder().groupByAlso("c").measure(k1Sum.getName(), k2Sum.getName()).build());
		Assertions.assertThat(MapBasedTabularView.load(secondView).getCoordinatesToValues())
				.containsEntry(Map.of("c", "c1"), Map.of(k1Sum.getName(), 0L + 123))
				.containsEntry(Map.of("c", "c2"), Map.of(k1Sum.getName(), 0L + 234, k2Sum.getName(), 0L + 345))
				.hasSize(2);

		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(1);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(2);
		});
	}

	@Test
	public void testCache_additionalMeasure_measureWithLessSlices_thenAddMeasureWithMoreSlices() {
		ITabularView firstView =
				cachingCube.execute(CubeQuery.builder().groupByAlso("c").measure(k2Sum.getName()).build());
		Assertions.assertThat(MapBasedTabularView.load(firstView).getCoordinatesToValues())
				.containsEntry(Map.of("c", "c2"), Map.of(k2Sum.getName(), 0L + 345))
				.hasSize(1);
		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(0);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(1);
		});

		ITabularView secondView = cachingCube
				.execute(CubeQuery.builder().groupByAlso("c").measure(k1Sum.getName(), k2Sum.getName()).build());
		Assertions.assertThat(MapBasedTabularView.load(secondView).getCoordinatesToValues())
				.containsEntry(Map.of("c", "c1"), Map.of(k1Sum.getName(), 0L + 123))
				.containsEntry(Map.of("c", "c2"), Map.of(k1Sum.getName(), 0L + 234, k2Sum.getName(), 0L + 345))
				.hasSize(2);

		Assertions.assertThat(caching.getCacheStats()).satisfies(cacheStats -> {
			Assertions.assertThat(cacheStats.hitCount()).isEqualTo(1);
			Assertions.assertThat(cacheStats.missCount()).isEqualTo(2);
		});
	}

	@AdhocBenchmark
	@Test
	public void testFuzzy() {
		List<CubeQuery> queries = new ArrayList<>();

		for (boolean withGroupBy : new boolean[] { true, false }) {
			for (boolean withFilter : new boolean[] { true, false }) {
				for (boolean withSumK1 : new boolean[] { true, false }) {
					for (boolean withSumK2 : new boolean[] { true, false }) {
						CubeQueryBuilder builder = CubeQuery.builder();

						if (withGroupBy) {
							builder.groupByAlso("c");
						}
						if (withFilter) {
							builder.andFilter("d", "d1");
						}
						if (withSumK1) {
							builder.measure(k1Sum.getAlias());
						}
						if (withSumK2) {
							builder.measure(k2Sum.getAlias());
						}

						queries.add(builder.build());
					}
				}
			}
		}

		for (int i = 0; i < queries.size(); i++) {
			for (int j = i + 1; j < queries.size(); j++) {
				caching.invalidateAll();

				CubeQuery firstQuery = queries.get(i);
				CubeQuery secondQuery = queries.get(j);

				// Second query without cache: this is the referential result
				MapBasedTabularView secondQueryNoCacheResult = MapBasedTabularView.load(cube().execute(secondQuery));

				// Do a first query feeding the cache
				cachingCube.execute(firstQuery);
				// Do a second query, given a fed cache
				MapBasedTabularView secondQueryCacheResult = MapBasedTabularView.load(cachingCube.execute(secondQuery));

				// Compare the results with and without cache
				Assertions.assertThat(secondQueryCacheResult).as("i=%s j=%s", i, j).isEqualTo(secondQueryNoCacheResult);
			}
		}
	}
}
