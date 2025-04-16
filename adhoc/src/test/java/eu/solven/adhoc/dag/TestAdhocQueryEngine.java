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
import org.junit.jupiter.api.Test;

import com.google.common.collect.Multimaps;
import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.dag.context.ExecutingQueryContext;
import eu.solven.adhoc.measure.UnsafeMeasureForestBag;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.query.cube.IAdhocQuery;
import eu.solven.adhoc.table.InMemoryTable;

public class TestAdhocQueryEngine {
	UnsafeMeasureForestBag forest = UnsafeMeasureForestBag.builder().name("engine").build();
	AdhocQueryEngine engine = AdhocQueryEngine.builder().eventBus(new EventBus()::post).build();

	@Test
	public void testColumnToAggregationKeys() {
		forest.addMeasure(Aggregator.builder().name("n1").columnName("c1").aggregationKey("A").build());
		forest.addMeasure(Aggregator.builder().name("n2").columnName("c1").aggregationKey("B").build());
		forest.addMeasure(Aggregator.builder().name("n3").aggregationKey("C").build());
		forest.addMeasure(Aggregator.builder().name("n4").build());

		IAdhocQuery query = AdhocQuery.builder().measures(forest.getNameToMeasure().values()).build();
		ExecutingQueryContext queryWithContext = ExecutingQueryContext.builder()
				.forest(forest)
				.query(query)
				.table(InMemoryTable.builder().build())
				.columnsManager(ColumnsManager.builder().build())
				.build();
		QueryStepsDag dag = engine.makeQueryStepsDag(queryWithContext);
		AdhocTableQueryEngine tableQueryEngine = engine.makeTableQueryEngine();
		Map<String, Set<Aggregator>> columnToAggregators =
				Multimaps.asMap(tableQueryEngine.columnToAggregators(queryWithContext, dag));

		Assertions.assertThat(columnToAggregators)
				.hasSize(3)
				.containsEntry("c1",
						Set.of((Aggregator) forest.getNameToMeasure().get("n1"),
								(Aggregator) forest.getNameToMeasure().get("n2")))
				.containsEntry("n3", Set.of((Aggregator) forest.getNameToMeasure().get("n3")))
				.containsEntry("n4", Set.of((Aggregator) forest.getNameToMeasure().get("n4")));
	}
}
