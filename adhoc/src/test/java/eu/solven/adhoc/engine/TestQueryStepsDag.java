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
package eu.solven.adhoc.engine;

import java.util.Arrays;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.UnsafeMeasureForest;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.table.InMemoryTable;

public class TestQueryStepsDag implements IAdhocTestConstants {
	CubeQueryEngine engine = CubeQueryEngine.builder().build();

	UnsafeMeasureForest measures = UnsafeMeasureForest.fromMeasures(this.getClass().getName(),
			Arrays.asList(k1Sum, k1SumSquared, filterK1onA1, filterK1onB1, shiftorAisA1));

	@Test
	public void testQuerySameMultipleTimes_Aggregator() {
		QueryStepsDag dag = engine.makeQueryStepsDag(QueryPod.builder()
				.query(CubeQuery.builder().measure(k1Sum, k1Sum).build())
				.forest(measures)
				.table(InMemoryTable.builder().build())
				.build());

		Assertions.assertThat(dag.getQueried()).hasSize(1);

		Assertions.assertThat(dag.getInducedToInducer().vertexSet()).hasSize(1);
		Assertions.assertThat(dag.getInducedToInducer().edgeSet()).hasSize(0);

		Assertions.assertThat(dag.underlyingSteps(CubeQueryStep.builder().measure(k1Sum).build())).isEmpty();
	}

	@Test
	public void testQuerySameMultipleTimes_Combinator() {

		QueryStepsDag dag = engine.makeQueryStepsDag(QueryPod.builder()
				.query(CubeQuery.builder().measure(k1SumSquared, k1SumSquared).build())
				.forest(measures)
				.table(InMemoryTable.builder().build())
				.build());

		Assertions.assertThat(dag.getQueried()).hasSize(1);

		Assertions.assertThat(dag.getInducedToInducer().vertexSet()).hasSize(2);
		Assertions.assertThat(dag.getInducedToInducer().edgeSet()).hasSize(1);

		Assertions.assertThat(dag.underlyingSteps(CubeQueryStep.builder().measure(k1SumSquared).build())).hasSize(1);
	}

	@Test
	public void testQuerySameMultipleTimes_Recursive() {
		measures.addMeasure(k1Sum);

		String timesNMinus1 = k1Sum.getName();
		String timesN = k1Sum.getName();
		for (int i = 0; i <= 2; i++) {
			if (i == 0) {
				timesNMinus1 = k1Sum.getName();
			} else {
				timesNMinus1 = k1Sum.getName() + "x" + (1 << i);
			}
			timesN = k1Sum.getName() + "x" + (1 << (i + 1));

			measures.addMeasure(Combinator.builder()
					.name(timesN)
					.underlyings(Arrays.asList(timesNMinus1, timesNMinus1))
					.combinationKey(SumCombination.KEY)
					.build());
		}

		QueryStepsDag dag = engine.makeQueryStepsDag(QueryPod.builder()
				.query(CubeQuery.builder().measure(timesN).build())
				.forest(measures)
				.table(InMemoryTable.builder().build())
				.build());

		Assertions.assertThat(dag.getQueried()).hasSize(1);

		Assertions.assertThat(dag.getInducedToInducer().vertexSet()).hasSize(4);
		Assertions.assertThat(dag.getInducedToInducer().edgeSet()).hasSize(3);

		Assertions
				.assertThat(dag.underlyingSteps(
						CubeQueryStep.builder().measure(measures.getNameToMeasure().get(timesN)).build()))
				.hasSize(2);

		Assertions
				.assertThat(dag.underlyingSteps(
						CubeQueryStep.builder().measure(measures.getNameToMeasure().get(timesNMinus1)).build()))
				.hasSize(2);
	}

	@Test
	public void testQueryParentAndChildren() {
		QueryStepsDag dag = engine.makeQueryStepsDag(QueryPod.builder()
				.query(CubeQuery.builder().measure(k1Sum, k1SumSquared).build())
				.forest(measures)
				.table(InMemoryTable.builder().build())
				.build());

		Assertions.assertThat(dag.getQueried()).hasSize(2);

		Assertions.assertThat(dag.getInducedToInducer().vertexSet()).hasSize(2);
		Assertions.assertThat(dag.getInducedToInducer().edgeSet()).hasSize(1);

		Assertions.assertThat(dag.underlyingSteps(CubeQueryStep.builder().measure(k1SumSquared).build())).hasSize(1);
	}

	@Test
	public void testQueryHasCommonChildren() {
		QueryStepsDag dag = engine.makeQueryStepsDag(QueryPod.builder()
				.query(CubeQuery.builder()
						.measure(filterK1onA1, filterK1onB1)
						.andFilter("a", "a1")
						.andFilter("b", "b1")
						.build())
				.forest(measures)
				.table(InMemoryTable.builder().build())
				.build());

		Assertions.assertThat(dag.getQueried()).hasSize(2);

		Assertions.assertThat(dag.getInducedToInducer().vertexSet()).hasSize(3);
		Assertions.assertThat(dag.getInducedToInducer().edgeSet()).hasSize(2);
	}

	@Test
	public void testMeasureHasTwiceSameUnderlying() {
		QueryStepsDag dag = engine.makeQueryStepsDag(QueryPod.builder()
				.query(CubeQuery.builder().measure(shiftorAisA1).andFilter("a", "a1").build())
				.forest(measures)
				.table(InMemoryTable.builder().build())
				.build());

		Assertions.assertThat(dag.getQueried()).hasSize(1);

		Assertions.assertThat(dag.getInducedToInducer().vertexSet()).hasSize(2);

		// There is 2 identical edges: they are merged in the DAG
		Assertions.assertThat(dag.getInducedToInducer().edgeSet()).hasSize(1);

		// The 2 identical edges are explicit in `underlyingSteps` with the help of a multigraph
		Assertions.assertThat(dag.underlyingSteps(
				CubeQueryStep.builder().measure(shiftorAisA1).filter(ColumnFilter.isEqualTo("a", "a1")).build()))
				.hasSize(2);

	}

	@Test
	public void testParallelPaths() {
		// This case is a measure leading through different paths, of different length, to the same node
		String mName = "k1+k1Squared";
		Combinator measure =
				Combinator.builder().name(mName).underlying(k1Sum.getName()).underlying(k1SumSquared.getName()).build();
		measures.addMeasure(measure);

		QueryStepsDag dag = engine.makeQueryStepsDag(QueryPod.builder()
				.query(CubeQuery.builder().measure(mName).build())
				.forest(measures)
				.table(InMemoryTable.builder().build())
				.build());

		Assertions.assertThat(dag.getQueried()).hasSize(1);

		Assertions.assertThat(dag.getInducedToInducer().vertexSet()).hasSize(3);
		Assertions.assertThat(dag.getInducedToInducer().edgeSet()).hasSize(3);

		Assertions.assertThat(dag.underlyingSteps(CubeQueryStep.builder().measure(measure).build())).hasSize(2);
	}

	@Test
	public void testQueryStepCache_root() {
		MapQueryStepCache queryStepCache = new MapQueryStepCache();

		String mName = k1Sum.getName();

		// Before filling the cache
		QueryStepsDag dagPre;
		{
			dagPre = engine.makeQueryStepsDag(QueryPod.builder()
					.query(CubeQuery.builder().measure(mName).build())
					.forest(measures)
					.table(InMemoryTable.builder().build())
					.queryStepCache(queryStepCache)
					.build());

			Assertions.assertThat(dagPre.getQueried()).hasSize(1);

			Assertions.assertThat(dagPre.getInducedToInducer().vertexSet()).hasSize(1);
			Assertions.assertThat(dagPre.getInducedToInducer().edgeSet()).hasSize(0);

			Assertions.assertThat(dagPre.underlyingSteps(CubeQueryStep.builder().measure(k1Sum).build())).isEmpty();

			Assertions.assertThat(dagPre.getStepToValues()).hasSize(0);
		}

		// Simulate the registration of a value in the cache
		ISliceToValue sliceToValue = SliceToValue.empty();
		queryStepCache.pushValues(Map.of(Iterables.getOnlyElement(dagPre.getQueried()), sliceToValue));
		Assertions.assertThat(queryStepCache.map).hasSize(1);

		// After filling the cache
		{
			QueryStepsDag dag = engine.makeQueryStepsDag(QueryPod.builder()
					.query(CubeQuery.builder().measure(mName).build())
					.forest(measures)
					.table(InMemoryTable.builder().build())
					.queryStepCache(queryStepCache)
					.build());

			Assertions.assertThat(dag.getQueried()).hasSize(1);

			Assertions.assertThat(dag.getInducedToInducer().vertexSet()).hasSize(1);
			Assertions.assertThat(dag.getInducedToInducer().edgeSet()).hasSize(0);

			Assertions.assertThat(dag.underlyingSteps(CubeQueryStep.builder().measure(k1Sum).build())).isEmpty();

			Assertions.assertThat(dag.getStepToValues()).hasSize(1);
		}
	}

	@Test
	public void testQueryStepCache_hasDependency_dependencyInCache() {
		MapQueryStepCache queryStepCache = new MapQueryStepCache();

		String mName = k1Sum.getName();

		QueryStepsDag dagPre;
		{
			dagPre = engine.makeQueryStepsDag(QueryPod.builder()
					.query(CubeQuery.builder().measure(mName).build())
					.forest(measures)
					.table(InMemoryTable.builder().build())
					.queryStepCache(queryStepCache)
					.build());

			Assertions.assertThat(dagPre.getQueried()).hasSize(1);

			Assertions.assertThat(dagPre.getInducedToInducer().vertexSet()).hasSize(1);
			Assertions.assertThat(dagPre.getInducedToInducer().edgeSet()).hasSize(0);

			Assertions.assertThat(dagPre.underlyingSteps(CubeQueryStep.builder().measure(k1Sum).build())).isEmpty();
		}

		// Simulate the registration of a value in the cache
		ISliceToValue sliceToValue = SliceToValue.empty();
		queryStepCache.pushValues(Map.of(Iterables.getOnlyElement(dagPre.getQueried()), sliceToValue));
		Assertions.assertThat(queryStepCache.map).hasSize(1);

		// After filling the cache with an underlying measure, we query a dependant measure
		{
			QueryStepsDag dag = engine.makeQueryStepsDag(QueryPod.builder()
					.query(CubeQuery.builder().measure(k1SumSquared.getName()).build())
					.forest(measures)
					.table(InMemoryTable.builder().build())
					.queryStepCache(queryStepCache)
					.build());

			Assertions.assertThat(dag.getQueried()).hasSize(1);

			// Dependant and underlying: the underlying has a value in cache
			Assertions.assertThat(dag.getInducedToInducer().vertexSet()).hasSize(2);
			Assertions.assertThat(dag.getInducedToInducer().edgeSet()).hasSize(1);

			Assertions.assertThat(dag.underlyingSteps(CubeQueryStep.builder().measure(k1SumSquared).build()))
					.hasSize(1);

			Assertions.assertThat(dag.getStepToValues())
					.containsKey(CubeQueryStep.builder().measure(k1Sum).build())
					.hasSize(1);
		}
	}

	@Test
	public void testQueryStepCache_hasDependency_dependentInCache() {
		MapQueryStepCache queryStepCache = new MapQueryStepCache();

		// Query the dependent with empty cache
		QueryStepsDag dagPre;
		{
			dagPre = engine.makeQueryStepsDag(QueryPod.builder()
					.query(CubeQuery.builder().measure(k1SumSquared).build())
					.forest(measures)
					.table(InMemoryTable.builder().build())
					.queryStepCache(queryStepCache)
					.build());

			Assertions.assertThat(dagPre.getQueried()).hasSize(1);

			Assertions.assertThat(dagPre.getInducedToInducer().vertexSet()).hasSize(2);
			Assertions.assertThat(dagPre.getInducedToInducer().edgeSet()).hasSize(1);

			Assertions.assertThat(dagPre.underlyingSteps(CubeQueryStep.builder().measure(k1SumSquared).build()))
					.hasSize(1);
		}

		// Simulate the registration of a value in the cache
		ISliceToValue sliceToValue = SliceToValue.empty();
		queryStepCache.pushValues(Map.of(Iterables.getOnlyElement(dagPre.getQueried()), sliceToValue));
		Assertions.assertThat(queryStepCache.map).hasSize(1);

		// After filling the cache with an underlying measure, we query a dependant measure
		{
			QueryStepsDag dag = engine.makeQueryStepsDag(QueryPod.builder()
					.query(CubeQuery.builder().measure(k1SumSquared.getName()).build())
					.forest(measures)
					.table(InMemoryTable.builder().build())
					.queryStepCache(queryStepCache)
					.build());

			Assertions.assertThat(dag.getQueried()).hasSize(1);

			// Given the dependent is in cache, the dependency is not added as vertex
			Assertions.assertThat(dag.getInducedToInducer().vertexSet()).hasSize(1);
			Assertions.assertThat(dag.getInducedToInducer().edgeSet()).hasSize(0);

			Assertions.assertThat(dag.underlyingSteps(CubeQueryStep.builder().measure(k1SumSquared).build()))
					.hasSize(0);

			Assertions.assertThat(dag.getStepToValues())
					.containsKey(CubeQueryStep.builder().measure(k1SumSquared).build())
					.hasSize(1);
		}
	}
}
