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
package eu.solven.adhoc.engine.tabular.splitter;

import java.util.Set;

import org.jgrapht.alg.TransitiveReduction;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.GraphHelpers;
import eu.solven.adhoc.engine.tabular.optimizer.IAdhocDag;
import eu.solven.adhoc.options.IHasQueryOptions;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Relates with {@link InduceByAdhocOptimistic} by registering all inducing options from one step to another (and not
 * picking one which may be the best a priori),
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@Builder
@RequiredArgsConstructor
public class InduceByAdhocComplete extends AInduceByAdhocParent implements IAddOnlyEdges {

	/**
	 * Given an input set of steps, callback on edge of an inducing DAG. The returned DAG looks for the edge which are
	 * the simplest one (e.g. given `a`, `a,b` and `a,b,c`, while `a` can induce both `a,b` and `a,b,c`, we prefer to
	 * have `a,b` as inducer of `a,b,c`).
	 * 
	 * This minimizing strategy helps minimizing the actual computations when reducing (e.g. it is easier to infer `a`
	 * given `a,b` than given `a,b,c`).
	 * 
	 * TODO it may not be possible/optimal to evaluate before the best inducingEdge. Typically, we may have `a,b,d` with
	 * only one row and `a,b,c` with 3 rows: `a,b,d` is a better candidate to infer `a`).
	 * 
	 * @param hasOptions
	 * @param inducedToInducer
	 * @return
	 */
	@Override
	@SuppressWarnings({ "PMD.CompareObjectsWithEquals", "checkstyle:AvoidInlineConditionals" })
	public IAdhocDag<TableQueryStep> splitInducedAsDag(IHasQueryOptions hasOptions,
			IAdhocDag<TableQueryStep> inducedToInducer) {
		Set<TableQueryStep> steps = inducedToInducer.vertexSet();
		if (steps.isEmpty()) {
			return GraphHelpers.makeGraph();
		}

		// groupBy number of groupedBy columns, in order to filter the candidate tableQueries
		// GroupBy tableQueries by groupBy cardinality, as we're guaranteed that a tableQuery with more groupBy can
		// not be inferred by a tableQUery with less groupBys.

		ListMultimap<Integer, TableQueryStep> cardinalityToSteps = steps.stream()
				.collect(Multimaps.toMultimap(s -> s.getGroupBy().getGroupedByColumns().size(),
						s -> s,
						ArrayListMultimap::create));

		int maxGroupBy = cardinalityToSteps.keySet().stream().mapToInt(i -> i).max().getAsInt();

		IAdhocDag<TableQueryStep> induceByAdhoc = GraphHelpers.makeGraph();

		// BEWARE Following algorithm is quadratic: for each step, we evaluate all other steps.
		// We observed up to 1k steps.
		// TODO SHould build inducing DAG for groupBy and filter independently, to help building faster
		steps.forEach(induced -> {
			induceByAdhoc.addVertex(induced);

			// inducer must have more groupBys than induced
			int smallestGroupBy = induced.getGroupBy().getGroupedByColumns().size();
			for (int inducerGroupBy = smallestGroupBy; inducerGroupBy <= maxGroupBy; inducerGroupBy++) {
				cardinalityToSteps.get(inducerGroupBy)
						.stream()
						// No edge to itself
						.filter(inducer -> inducer != induced)
						// Compatible context (i.e. laxer filter, customMarker, options)
						.filter(inducer -> canInduce(inducer, induced))
						// Consider all nodes: hence, at runtime, we will wait to fetch all possible inducers and
						// decide
						// for the optimal one
						.forEach(inducer -> {
							// right can be used to compute left
							induceByAdhoc.addVertex(inducer);
							induceByAdhoc.addEdge(induced, inducer);
							log.debug("Induced -> Inducer ({} -> {})", induced, inducer);
						});
			}

		});

		// TODO Improve the steps generation to prevent adding too many edges to later remove
		TransitiveReduction.INSTANCE.reduce(induceByAdhoc);

		return induceByAdhoc;
	}

}
