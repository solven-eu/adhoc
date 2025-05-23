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
package eu.solven.adhoc.measure.graphviz;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.MeasureForest.MeasureForestBuilder;
import eu.solven.adhoc.measure.examples.RatioOverCurrentColumnValueCompositor;
import eu.solven.adhoc.measure.examples.RatioOverSpecificColumnValueCompositor;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;
import guru.nidi.graphviz.model.MutableGraph;

public class TestForestAsGraphvizDag {
	@Test
	public void testEmpty() {
		ForestAsGraphvizDag graphviz = ForestAsGraphvizDag.builder().build();

		IMeasureForest forest = MeasureForest.empty();
		MutableGraph graph = graphviz.asGraph(forest);

		Assertions.assertThat(graph.toString()).isEqualTo("""
				digraph "forest=empty" {
				graph ["rankdir"="LR","label"="forest=empty"]
				node ["fontname"="arial"]
				edge ["class"="link-class"]
				}""");
	}

	@Test
	public void testOnlyAggregators() {
		ForestAsGraphvizDag graphviz = ForestAsGraphvizDag.builder().build();

		IMeasureForest forest = MeasureForest.builder()
				.name(this.getClass().getSimpleName())
				.measure(Aggregator.countAsterisk())
				.measure(Aggregator.sum("simplestSum"))
				.build();
		MutableGraph graph = graphviz.asGraph(forest);

		Assertions.assertThat(graph.toString()).isEqualTo("""
				digraph "forest=TestForestAsGraphvizDag" {
				graph ["rankdir"="LR","label"="forest=TestForestAsGraphvizDag"]
				node ["fontname"="arial"]
				edge ["class"="link-class"]
				"count(*)"
				"simplestSum"
				}""");
	}

	@Test
	public void testDeepCombinator() {
		ForestAsGraphvizDag graphviz = ForestAsGraphvizDag.builder().build();

		MeasureForestBuilder measureForestBuilder =
				MeasureForest.builder().name(this.getClass().getSimpleName()).measure(Aggregator.countAsterisk());

		String baseName = Aggregator.countAsterisk().getName();
		for (int i = 1; i < 4; i++) {
			String underlyingName = baseName + "_x" + (1 << (i - 1));
			measureForestBuilder.measure(Combinator.builder()
					.name(baseName + "_x" + (1 << i))
					.combinationKey(SumCombination.KEY)
					.underlying(underlyingName)
					.underlying(underlyingName)
					.build());
		}

		IMeasureForest forest = measureForestBuilder.build();
		MutableGraph graph = graphviz.asGraph(forest);

		// Graphviz dot = Graphviz.fromGraph(graph);

		Assertions.assertThat(graph.toString()).isEqualTo("""
				digraph "forest=TestForestAsGraphvizDag" {
				graph ["rankdir"="LR","label"="forest=TestForestAsGraphvizDag"]
				node ["fontname"="arial"]
				edge ["class"="link-class"]
				"count(*)_x4" ["fillcolor"="cyan","style"="filled"]
				"count(*)_x2" ["fillcolor"="cyan","style"="filled"]
				"count(*)_x8" ["fillcolor"="cyan","style"="filled"]
				"count(*)"
				"count(*)_x4" -> "count(*)_x2"
				"count(*)_x4" -> "count(*)_x2"
				"count(*)_x2" -> "count(*)_x1"
				"count(*)_x2" -> "count(*)_x1"
				"count(*)_x8" -> "count(*)_x4"
				"count(*)_x8" -> "count(*)_x4"
				}""");
	}

	@Test
	public void testRatioOverSpecificColumnValueCompositor() {
		ForestAsGraphvizDag graphviz = ForestAsGraphvizDag.builder().build();

		MeasureForestBuilder measureForestBuilder = MeasureForest.builder().name(this.getClass().getSimpleName());

		measureForestBuilder.measure(Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build());

		IMeasureForest forest = measureForestBuilder.build()
				.acceptVisitor(new RatioOverSpecificColumnValueCompositor().asCombinator("country", "FR", "d"));

		MutableGraph graph = graphviz.asGraph(forest);

		Assertions.assertThat(graph.toString()).isEqualTo("""
				digraph "forest=TestForestAsGraphvizDag" {
				graph ["rankdir"="LR","label"="forest=TestForestAsGraphvizDag"]
				node ["fontname"="arial"]
				edge ["class"="link-class"]
				"d_country=FR_ratio" ["fillcolor"="cyan","style"="filled"]
				"d_country=FR_slice" ["fillcolor"="grey","style"="filled"]
				"d"
				"d_country=FR_ratio" -> "d_country=FR_slice"
				"d_country=FR_ratio" -> "d_country=FR_whole"
				"d_country=FR_slice" -> "d"
				"d_country=FR_whole" -> "d_country=FR_slice"
				}""");
	}

	@Test
	public void testRatioOverCurrentColumnValueCompositor() {
		ForestAsGraphvizDag graphviz = ForestAsGraphvizDag.builder().build();

		MeasureForestBuilder measureForestBuilder = MeasureForest.builder().name(this.getClass().getSimpleName());

		measureForestBuilder.measure(Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build());

		IMeasureForest forest = measureForestBuilder.build()
				.acceptVisitor(new RatioOverCurrentColumnValueCompositor().asCombinator("country", "d"));

		MutableGraph graph = graphviz.asGraph(forest);

		Assertions.assertThat(graph.toString()).isEqualTo("""
				digraph "forest=TestForestAsGraphvizDag" {
				graph ["rankdir"="LR","label"="forest=TestForestAsGraphvizDag"]
				node ["fontname"="arial"]
				edge ["class"="link-class"]
				"d_country=current_ratio_postcheck" ["fillcolor"="cyan","style"="filled"]
				"d_country=current_slice" ["shape"="star","fixedsize"="true","fillcolor"="yellow","style"="filled"]
				"d_country=current_ratio_postcheck" -> "d_country=current_slice"
				"d_country=current_ratio_postcheck" -> "d_country=current_whole"
				"d_country=current_slice" -> "d"
				"d_country=current_whole" -> "d_country=current_slice"
				"d_country=current_ratio" -> "d_country=current_ratio_postcheck"
				}""");
	}
}
