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
package eu.solven.adhoc.dag;

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.greenrobot.eventbus.EventBus;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.transformers.Aggregator;

public class TestAdhocQueryEngine {
	AdhocMeasureBag amg = AdhocMeasureBag.builder().build();
	AdhocQueryEngine aqe = AdhocQueryEngine.builder().eventBus(new EventBus()::post).build();

	@Test
	public void testColumnToAggregationKeys() {
		amg.addMeasure(Aggregator.builder().name("n1").columnName("c1").aggregationKey("A").build());
		amg.addMeasure(Aggregator.builder().name("n2").columnName("c1").aggregationKey("B").build());
		amg.addMeasure(Aggregator.builder().name("n3").aggregationKey("C").build());
		amg.addMeasure(Aggregator.builder().name("n4").build());

		IAdhocQuery adhocQuery = AdhocQuery.builder().measures(amg.getNameToMeasure().keySet()).build();
		AdhocExecutingQueryContext queryWithContext =
				AdhocExecutingQueryContext.builder().measureBag(amg).adhocQuery(adhocQuery).build();
		DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates =
				aqe.makeQueryStepsDag(queryWithContext);
		Map<String, Set<Aggregator>> columnToAggregators =
				aqe.columnToAggregators(queryWithContext, fromQueriedToAggregates);

		Assertions.assertThat(columnToAggregators)
				.hasSize(3)
				.containsEntry("c1",
						Set.of((Aggregator) amg.getNameToMeasure().get("n1"),
								(Aggregator) amg.getNameToMeasure().get("n2")))
				.containsEntry("n3", Set.of((Aggregator) amg.getNameToMeasure().get("n3")))
				.containsEntry("n4", Set.of((Aggregator) amg.getNameToMeasure().get("n4")));
	}
}
