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
package eu.solven.adhoc.measure.mermaid;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.examples.RatioOverCurrentColumnValueCompositor;
import eu.solven.adhoc.measure.examples.RatioOverSpecificColumnValueCompositor;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.forest.MeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.resource.MeasureForests;

public class TestForestAsMermaidDag {
	@Test
	public void testEmpty() {
		ForestAsMermaidDag mermaid = ForestAsMermaidDag.builder().build();

		IMeasureForest forest = MeasureForest.empty();
		String out = mermaid.asMermaid(forest);

		Assertions.assertThat(out).isEqualTo("""
				flowchart LR
				""");
	}

	@Test
	public void testOnlyAggregators() {
		ForestAsMermaidDag mermaid = ForestAsMermaidDag.builder().build();

		IMeasureForest forest = MeasureForest.builder()
				.name(this.getClass().getSimpleName())
				.measure(Aggregator.countAsterisk())
				.measure(Aggregator.sum("simplestSum"))
				.build();
		String out = mermaid.asMermaid(forest);

		Assertions.assertThat(out).isEqualTo("""
				flowchart LR
				    id0[("count(*)")]
				    style id0 fill:#ff9b86
				    id1[("simplestSum")]
				    style id1 fill:#ff9b86
				""");
	}

	@Test
	public void testDeepCombinator() {
		ForestAsMermaidDag mermaid = ForestAsMermaidDag.builder().build();

		MeasureForest.MeasureForestBuilder measureForestBuilder =
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
		String out = mermaid.asMermaid(forest);

		Assertions.assertThat(out).isEqualTo("""
				flowchart LR
				    id0[("count(*)")]
				    style id0 fill:#ff9b86
				    id1("count(*)_x2")
				    style id1 fill:#a6e3f5
				    id2("count(*)_x4")
				    style id2 fill:#a6e3f5
				    id3("count(*)_x8")
				    style id3 fill:#a6e3f5
				    id1 --> id4
				    id1 --> id4
				    id2 --> id1
				    id2 --> id1
				    id3 --> id2
				    id3 --> id2
				""");
	}

	@Test
	public void testRatioOverSpecificColumnValueCompositor() {
		ForestAsMermaidDag mermaid = ForestAsMermaidDag.builder().build();

		MeasureForest.MeasureForestBuilder measureForestBuilder =
				MeasureForest.builder().name(this.getClass().getSimpleName());

		measureForestBuilder.measure(Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build());

		IMeasureForest forest = measureForestBuilder.build()
				.acceptVisitor(new RatioOverSpecificColumnValueCompositor().asCombinator("country", "FR", "d"));

		String out = mermaid.asMermaid(forest);

		Assertions.assertThat(out).contains("flowchart LR", "id0[(\"d\")]", "style id0 fill:#ff9b86");
		// Filtrator for the FR slice
		Assertions.assertThat(out).contains("[\\\"d_country=FR_slice\"\\]", "fill:#a6c9a2");
		// Combinator for the ratio
		Assertions.assertThat(out).contains("(\"d_country=FR_ratio\")", "fill:#a6e3f5");
		// Expected edges must all be present
		Assertions.assertThat(out).contains(" --> ");
	}

	@Test
	public void testRatioOverCurrentColumnValueCompositor() {
		ForestAsMermaidDag mermaid = ForestAsMermaidDag.builder().build();

		MeasureForest.MeasureForestBuilder measureForestBuilder =
				MeasureForest.builder().name(this.getClass().getSimpleName());

		measureForestBuilder.measure(Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build());

		IMeasureForest forest = measureForestBuilder.build()
				.acceptVisitor(new RatioOverCurrentColumnValueCompositor().asCombinator("country", "d"));

		String out = mermaid.asMermaid(forest);

		Assertions.assertThat(out).contains("flowchart LR", "id0[(\"d\")]", "style id0 fill:#ff9b86");
		// The Partitionor slice uses the hexagon shape and the yellow fill
		Assertions.assertThat(out).contains("{{\"d_country=current_slice\"}}", "fill:#ffef8a");
	}

	@Test
	public void testHighlightedMeasures() {
		ForestAsMermaidDag mermaid =
				ForestAsMermaidDag.builder().highlightedMeasures(Set.of("simplestSum", "notInForest")).build();

		IMeasureForest forest = MeasureForest.builder()
				.name(this.getClass().getSimpleName())
				.measure(Aggregator.countAsterisk())
				.measure(Aggregator.sum("simplestSum"))
				.build();
		String out = mermaid.asMermaid(forest);

		Assertions.assertThat(out).isEqualTo("""
				flowchart LR
				    id0[("count(*)")]
				    style id0 fill:#ff9b86
				    id1[("simplestSum")]
				    style id1 fill:#ff9b86,stroke:red,stroke-width:3px
				""");
	}

	@Test
	public void testDispatchor() {
		ForestAsMermaidDag mermaid = ForestAsMermaidDag.builder().build();

		MeasureForest.MeasureForestBuilder measureForestBuilder =
				MeasureForest.builder().name(this.getClass().getSimpleName());

		measureForestBuilder.measure(Aggregator.builder().name("d").aggregationKey(SumAggregation.KEY).build());

		measureForestBuilder.measure(
				Dispatchor.builder().name("dispatched").decompositionKey("dispatchKey").underlying("d").build());

		MeasureForest forest = measureForestBuilder.build();
		String out = mermaid.asMermaid(forest);

		Assertions.assertThat(out).isEqualTo("""
				flowchart LR
				    id0[("d")]
				    style id0 fill:#ff9b86
				    id1[["dispatched"]]
				    style id1 fill:#c8c8c8
				    id1 --> id0
				""");
	}

	@Test
	public void testMultipleForests() {
		ForestAsMermaidDag mermaid = ForestAsMermaidDag.builder().build();

		IMeasureForest forestA = MeasureForest.builder().name("A").measure(Aggregator.countAsterisk()).build();
		IMeasureForest forestB = MeasureForest.builder().name("B").measure(Aggregator.sum("simplestSum")).build();

		MeasureForests forests = MeasureForests.builder().forest(forestA).forest(forestB).build();

		String out = mermaid.asMermaid(forests);

		Assertions.assertThat(out).isEqualTo("""
				flowchart LR
				    subgraph "forest=A"
				        id0[("count(*)")]
				        style id0 fill:#ff9b86
				    end
				    subgraph "forest=B"
				        id1[("simplestSum")]
				        style id1 fill:#ff9b86
				    end
				""");
	}
}
