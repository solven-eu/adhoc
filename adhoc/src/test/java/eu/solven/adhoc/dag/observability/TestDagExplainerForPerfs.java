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
package eu.solven.adhoc.dag.observability;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.dag.ICanResolveMeasure;
import eu.solven.adhoc.dag.QueryStepsDag;
import eu.solven.adhoc.dag.QueryStepsDagBuilder;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.ratio.AdhocExplainerTestHelper;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.cube.CubeQuery;

public class TestDagExplainerForPerfs {
	EventBus eventBus = new EventBus();
	List<String> messages = AdhocExplainerTestHelper.listenForPerf(eventBus);

	@Test
	public void testPerfLog() {
		DagExplainerForPerfs dagExplainer = DagExplainerForPerfs.builder().eventBus(eventBus::post).build();

		QueryStepsDagBuilder queryStepsDagBuilder =
				new QueryStepsDagBuilder(new StandardOperatorsFactory(), "someCube", CubeQuery.builder().build());

		Map<String, IMeasure> refToMeasure = new HashMap<>();

		ICanResolveMeasure canResolve = m -> {
			if (m instanceof ReferencedMeasure ref) {
				return refToMeasure.get(ref.getRef());
			} else {
				return m;
			}
		};

		Combinator root = Combinator.builder().name("root").underlying("underlying1").underlying("underlying2").build();

		Combinator underlying1 =
				Combinator.builder().name("underlying1").underlying("underlying11").underlying("underlying12").build();
		Combinator underlying2 =
				Combinator.builder().name("underlying2").underlying("underlying21").underlying("underlying22").build();

		Combinator underlying11 = Combinator.builder().name("underlying11").underlying("a").build();
		Combinator underlying12 = Combinator.builder().name("underlying12").underlying("a").build();
		Combinator underlying21 = Combinator.builder().name("underlying21").underlying("a").build();
		Combinator underlying22 = Combinator.builder().name("underlying22").underlying("a").build();

		Aggregator aggregator111 = Aggregator.sum("a");

		refToMeasure.put("underlying1", underlying1);
		refToMeasure.put("underlying2", underlying2);
		refToMeasure.put("underlying11", underlying11);
		refToMeasure.put("underlying12", underlying12);
		refToMeasure.put("underlying21", underlying21);
		refToMeasure.put("underlying22", underlying22);
		refToMeasure.put("a", aggregator111);

		queryStepsDagBuilder.registerRootWithUnderlyings(canResolve, Set.of(root));

		QueryStepsDag dag = queryStepsDagBuilder.getQueryDag();

		dagExplainer.explain(AdhocQueryId.from("someCube", CubeQuery.builder().build()), dag);

		Assertions.assertThat(messages.stream().collect(Collectors.joining("\n"))).isEqualTo("""
				#0 s=someCube id=00000000-0000-0000-0000-000000000000
				|  No cost info
				\\-- #1 m=root(Combinator[SUM]) filter=matchAll groupBy=grandTotal
				    |  No cost info
				    |\\- #2 m=underlying1(Combinator[SUM]) filter=matchAll groupBy=grandTotal
				    |   |  No cost info
				    |   |\\- #3 m=underlying11(Combinator[SUM]) filter=matchAll groupBy=grandTotal
				    |   |   |  No cost info
				    |   |   \\-- #4 m=a(SUM) filter=matchAll groupBy=grandTotal
				    |   |       \\  No cost info
				    |   \\-- #5 m=underlying12(Combinator[SUM]) filter=matchAll groupBy=grandTotal
				    |       |  No cost info
				    |       \\-- !4
				    \\-- #6 m=underlying2(Combinator[SUM]) filter=matchAll groupBy=grandTotal
				        |  No cost info
				        |\\- #7 m=underlying21(Combinator[SUM]) filter=matchAll groupBy=grandTotal
				        |   |  No cost info
				        |   \\-- !4
				        \\-- #8 m=underlying22(Combinator[SUM]) filter=matchAll groupBy=grandTotal
				            |  No cost info
				            \\-- !4""");
	}
}
