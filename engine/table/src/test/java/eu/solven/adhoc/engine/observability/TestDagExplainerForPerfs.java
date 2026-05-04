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
package eu.solven.adhoc.engine.observability;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.engine.QueryStepsDag;
import eu.solven.adhoc.engine.QueryStepsDagBuilder;
import eu.solven.adhoc.engine.cache.IQueryStepCache;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.factories.AdhocFactories;
import eu.solven.adhoc.measure.forest.IMeasureResolver;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.IMeasure;
import eu.solven.adhoc.model.measure.ReferencedMeasure;
import eu.solven.adhoc.query.AdhocQueryIds;

public class TestDagExplainerForPerfs {
	EventBus eventBus = new EventBus();
	List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBus);

	@Test
	public void testPerfLog() {
		DagExplainerForPerfs dagExplainer = DagExplainerForPerfs.builder().eventBus(eventBus::post).build();

		Map<String, IMeasure> refToMeasure = new HashMap<>();

		IMeasureResolver canResolve = m -> {
			if (m instanceof ReferencedMeasure ref) {
				return refToMeasure.get(ref.getRef());
			} else {
				return m;
			}
		};

		QueryStepsDagBuilder queryStepsDagBuilder = new QueryStepsDagBuilder(AdhocFactories.builder().build(),
				"someCube",
				canResolve,
				CubeQuery.builder().measure("someMeasure").build(),
				IQueryStepCache.noCache());

		ObservabilityCombinator root = ObservabilityCombinator.builder()
				.name("root")
				.underlying("underlying1")
				.underlying("underlying2")
				.build();

		ObservabilityCombinator underlying1 = ObservabilityCombinator.builder()
				.name("underlying1")
				.underlying("underlying11")
				.underlying("underlying12")
				.build();
		ObservabilityCombinator underlying2 = ObservabilityCombinator.builder()
				.name("underlying2")
				.underlying("underlying21")
				.underlying("underlying22")
				.build();

		ObservabilityCombinator underlying11 =
				ObservabilityCombinator.builder().name("underlying11").underlying("a").build();
		ObservabilityCombinator underlying12 =
				ObservabilityCombinator.builder().name("underlying12").underlying("a").build();
		ObservabilityCombinator underlying21 =
				ObservabilityCombinator.builder().name("underlying21").underlying("a").build();
		ObservabilityCombinator underlying22 =
				ObservabilityCombinator.builder().name("underlying22").underlying("a").build();

		Aggregator aggregator111 = Aggregator.sum("a");

		refToMeasure.put("underlying1", underlying1);
		refToMeasure.put("underlying2", underlying2);
		refToMeasure.put("underlying11", underlying11);
		refToMeasure.put("underlying12", underlying12);
		refToMeasure.put("underlying21", underlying21);
		refToMeasure.put("underlying22", underlying22);
		refToMeasure.put("a", aggregator111);

		queryStepsDagBuilder.registerRootWithDescendants(Set.of(root));

		QueryStepsDag dag = queryStepsDagBuilder.getQueryDag();

		dagExplainer.explain(AdhocQueryIds.from("someCube", "someQueryObject"), dag);

		Assertions.assertThat(String.join("\n", messages)).isEqualToNormalizingNewlines("""
				/-- #0 c=someCube id=00000000-0000-0000-0000-000000000000
				|      No cost info
				\\-- #1 m=root(ObservabilityCombinator[SUM]) filter=matchAll groupBy=grandTotal
				    |  No cost info
				    |\\- #2 m=underlying1(ObservabilityCombinator[SUM]) filter=matchAll groupBy=grandTotal
				    |   |  No cost info
				    |   |\\- #3 m=underlying11(ObservabilityCombinator[SUM]) filter=matchAll groupBy=grandTotal
				    |   |   |  No cost info
				    |   |   \\-- #4 m=a(SUM) filter=matchAll groupBy=grandTotal
				    |   |       \\  No cost info
				    |   \\-- #5 m=underlying12(ObservabilityCombinator[SUM]) filter=matchAll groupBy=grandTotal
				    |       |  No cost info
				    |       \\-- !4
				    \\-- #6 m=underlying2(ObservabilityCombinator[SUM]) filter=matchAll groupBy=grandTotal
				        |  No cost info
				        |\\- #7 m=underlying21(ObservabilityCombinator[SUM]) filter=matchAll groupBy=grandTotal
				        |   |  No cost info
				        |   \\-- !4
				        \\-- #8 m=underlying22(ObservabilityCombinator[SUM]) filter=matchAll groupBy=grandTotal
				            |  No cost info
				            \\-- !4""");
	}
}
