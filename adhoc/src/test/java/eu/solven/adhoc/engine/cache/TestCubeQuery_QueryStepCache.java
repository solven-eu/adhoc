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
package eu.solven.adhoc.engine.cache;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.context.IQueryPreparator;
import eu.solven.adhoc.engine.context.StandardQueryPreparator;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.query.cube.CubeQuery;

public class TestCubeQuery_QueryStepCache extends ADagTest implements IAdhocTestConstants {

	@BeforeEach
	@Override
	public void feedTable() {
		table().add(Map.of("c", "c1", "d", "d1", "k1", 123));
		table().add(Map.of("c", "c1", "d", "d2", "k1", 234));
		table().add(Map.of("c", "c2", "d", "d1", "k1", 345, "k2", 456));
		table().add(Map.of("c", "c2", "d", "d2", "k2", 567));
	}

	@BeforeEach
	public void addMeasures() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);
		forest.addMeasure(k1PlusK2AsExpr);
	}

	final GuavaQueryStepCache cache = GuavaQueryStepCache.withSize(1);

	@Override
	protected IQueryPreparator queryPreparator() {
		return StandardQueryPreparator.builder().queryStepCache(cache).build();
	}

	@Test
	public void testBeforeQuery() {
		Assertions.assertThat(cache.queryStepToValues.stats().hitCount()).isEqualTo(0L);
		Assertions.assertThat(cache.queryStepToValues.stats().missCount()).isEqualTo(0L);
	}

	@Test
	public void testGrandTotal_aggregator() {
		// first try: cache empty
		{
			ITabularView output = cube().execute(CubeQuery.builder().measure(k1Sum).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(output);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.hasSize(1)
					.containsEntry(Collections.emptyMap(), Map.of(k1Sum.getName(), 0L + 123 + 234 + 345));
		}
		Assertions.assertThat(cache.queryStepToValues.stats().hitCount()).isEqualTo(0L);
		Assertions.assertThat(cache.queryStepToValues.stats().missCount()).isEqualTo(1L);

		// second try: cache full
		{
			ITabularView output = cube().execute(CubeQuery.builder().measure(k1Sum).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(output);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.hasSize(1)
					.containsEntry(Collections.emptyMap(), Map.of(k1Sum.getName(), 0L + 123 + 234 + 345));
		}
		Assertions.assertThat(cache.queryStepToValues.stats().hitCount()).isEqualTo(1L);
		Assertions.assertThat(cache.queryStepToValues.stats().missCount()).isEqualTo(1L);
	}

	@Test
	public void testGrandTotal_transformator() {
		// first try: cache empty
		{
			ITabularView output = cube().execute(CubeQuery.builder().measure(k1PlusK2AsExpr).explain(true).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(output);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.hasSize(1)
					.containsEntry(Collections.emptyMap(),
							Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 345 + 456 + 567));

			Assertions.assertThat(cache.queryStepToValues.stats().hitCount()).isEqualTo(0L);
			// 3 entries: `k1`, `k2`, `k1+k2`
			Assertions.assertThat(cache.queryStepToValues.stats().missCount()).isEqualTo(3L);
			Assertions.assertThat(cache.queryStepToValues.size()).isEqualTo(1L);
			Assertions.assertThat(cache.queryStepToValues.asMap().keySet()).anySatisfy(step -> {
				Assertions.assertThat(step.getMeasure().getName()).isEqualTo(k1PlusK2AsExpr.getName());
			});
		}

		// second try: cache full
		List<String> messages = AdhocExplainerTestHelper.listenForExplainNoPerf(eventBus);
		{
			ITabularView output = cube().execute(CubeQuery.builder().measure(k1PlusK2AsExpr).explain(true).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(output);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.hasSize(1)
					.containsEntry(Collections.emptyMap(),
							Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 345 + 456 + 567));

			// 1 entry: `k1+k2`
			Assertions.assertThat(cache.queryStepToValues.stats().hitCount()).isEqualTo(1L);
			Assertions.assertThat(cache.queryStepToValues.stats().missCount()).isEqualTo(3L);

			Assertions.assertThat(cache.queryStepToValues.size()).isEqualTo(1L);
			Assertions.assertThat(cache.queryStepToValues.asMap().keySet()).anySatisfy(step -> {
				Assertions.assertThat(step.getMeasure().getName()).isEqualTo(k1PlusK2AsExpr.getName());
			});
		}
		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n")))
				.isEqualTo(
						"""
								/-- #0 c=inMemory id=00000000-0000-0000-0000-000000000000
								\\-- #1 m=k1PlusK2AsExpr(Combinator[EXPRESSION]) filter=matchAll groupBy=grandTotal
								/-- #0 t=inMemory id=00000000-0000-0000-0000-000000000001 (parentId=00000000-0000-0000-0000-000000000000)""")
				.hasLineCount(2 + 1);
	}

	@Test
	public void testGrandTotal_aggregatorThenTransformator() {
		// first try: cache empty
		{
			ITabularView output = cube().execute(CubeQuery.builder().measure(k1Sum).explain(true).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(output);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.hasSize(1)
					.containsEntry(Collections.emptyMap(), Map.of(k1Sum.getName(), 0L + 123 + 234 + 345));

			Assertions.assertThat(cache.queryStepToValues.stats().hitCount()).isEqualTo(0L);
			// 1 entries: `k1`
			Assertions.assertThat(cache.queryStepToValues.stats().missCount()).isEqualTo(1L);
			Assertions.assertThat(cache.queryStepToValues.size()).isEqualTo(1L);
			Assertions.assertThat(cache.queryStepToValues.asMap().keySet()).anySatisfy(step -> {
				Assertions.assertThat(step.getMeasure().getName()).isEqualTo(k1Sum.getName());
			});
		}

		// second try: cache partially full
		List<String> messages = AdhocExplainerTestHelper.listenForExplainNoPerf(eventBus);
		{
			ITabularView output = cube().execute(CubeQuery.builder().measure(k1PlusK2AsExpr).explain(true).build());
			MapBasedTabularView mapBased = MapBasedTabularView.load(output);

			Assertions.assertThat(mapBased.getCoordinatesToValues())
					.hasSize(1)
					.containsEntry(Collections.emptyMap(),
							Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 234 + 345 + 456 + 567));

			// 1 entry: `k1+k2`
			Assertions.assertThat(cache.queryStepToValues.stats().hitCount()).isEqualTo(1L);
			// As `cacheSize==1`, we missed `k1+k2` and `k2`, on top of `k1` missing on previous query
			Assertions.assertThat(cache.queryStepToValues.stats().missCount()).isEqualTo(3L);
			Assertions.assertThat(cache.queryStepToValues.size()).isEqualTo(1L);
			Assertions.assertThat(cache.queryStepToValues.asMap().keySet()).anySatisfy(step -> {
				Assertions.assertThat(step.getMeasure().getName()).isEqualTo(k1PlusK2AsExpr.getName());
			});
		}
		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n")))
				.isEqualTo(
						"""
								/-- #0 c=inMemory id=00000000-0000-0000-0000-000000000000
								\\-- #1 m=k1PlusK2AsExpr(Combinator[EXPRESSION]) filter=matchAll groupBy=grandTotal
								    |\\- #2 m=k1(SUM) filter=matchAll groupBy=grandTotal
								    \\-- #3 m=k2(SUM) filter=matchAll groupBy=grandTotal
								/-- 1 inducers from SELECT k2:SUM(k2) GROUP BY ()
								\\-- step SELECT k2:SUM(k2) GROUP BY ()
								/-- #0 t=inMemory id=00000000-0000-0000-0000-000000000001 (parentId=00000000-0000-0000-0000-000000000000)
								\\-- #1 m=k2(SUM) filter=matchAll groupBy=grandTotal""");

		Assertions.assertThat(messages).hasSize(4 + 2 + 2);
	}

}
