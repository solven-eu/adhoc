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
package eu.solven.adhoc.engine.tabular.optimizer.merger;

import java.util.Map;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.dag.GraphHelpers;
import eu.solven.adhoc.engine.dag.IAdhocDag;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.optimizer.GraphsTestHelpers;
import eu.solven.adhoc.engine.tabular.splitter.adder.AddSharedNodes;
import eu.solven.adhoc.engine.tabular.splitter.adder.IAddSharedNodes;
import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.value.InMatcher;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestAddSharedNodes {
	IAddSharedNodes sharedNodes = AddSharedNodes.builder().build();

	TableQueryStep a = TableQueryStep.builder().aggregator(Aggregator.sum("k")).build();

	@Test
	public void sharedFilter_sharedWithLessGroupBy() {
		TableQueryStep s1 = a.toBuilder().filter(AndFilter.and("a", "a1")).groupBy(GroupByColumns.named("a")).build();
		TableQueryStep s2 = a.toBuilder().filter(AndFilter.and("a", "a1")).groupBy(GroupByColumns.named("b")).build();
		TableQueryStep s3 =
				a.toBuilder().filter(AndFilter.and(Map.of())).groupBy(GroupByColumns.named("a", "b", "c")).build();

		IAdhocDag<TableQueryStep> merged = GraphHelpers.makeGraph();
		merged.addVertex(s1);
		merged.addVertex(s2);
		merged.addVertex(s3);

		merged.addEdge(s1, s3);
		merged.addEdge(s2, s3);

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(merged);

		TableQueryStep shared =
				a.toBuilder().filter(AndFilter.and("a", "a1")).groupBy(GroupByColumns.named("a", "b")).build();
		Assertions.assertThat(withShared.vertexSet()).hasSize(4).contains(s1, s2, s3).contains(shared);

		Assertions.assertThat(withShared.edgeSet())
				.hasSize(3)
				.anySatisfy(GraphsTestHelpers.assertEdge(s1, shared, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s2, shared, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(shared, s3, withShared));
	}

	// Given `a->a,b,c`, `a,b->a,b,c`, `a,c->a,b,c`
	// We should remove `a->a,b,c` and add both `a->a,c` and `a->a,b`
	@Test
	public void chainOfInducers_groupBy() {
		TableQueryStep s1 = a.toBuilder().groupBy(GroupByColumns.named("a")).build();
		TableQueryStep s2 = a.toBuilder().groupBy(GroupByColumns.named("a", "b")).build();
		TableQueryStep s3 = a.toBuilder().groupBy(GroupByColumns.named("a", "c")).build();

		TableQueryStep globalInducer = a.toBuilder().groupBy(GroupByColumns.named("a", "b", "c")).build();

		IAdhocDag<TableQueryStep> merged = GraphHelpers.makeGraph();
		merged.addVertex(globalInducer);
		Stream.of(s1, s2, s3).forEach(s -> {
			merged.addVertex(s);
			merged.addEdge(s, globalInducer);
		});

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(merged);

		Assertions.assertThat(withShared.vertexSet()).hasSize(4).contains(s1, s2, s3).contains(globalInducer);

		Assertions.assertThat(withShared.edgeSet())
				.hasSize(3)
				.anySatisfy(GraphsTestHelpers.assertEdge(s1, globalInducer, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s2, globalInducer, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s3, globalInducer, withShared));
	}

	// Given `FILTER a1`, `FILTER a1 OR a2`, `FILTER a1 OR a2 OR a3`
	// We should infer `FILTER a1` given `FILTER a1 OR a2`, itself given `FILTER a1 OR a2 OR a3`
	@Test
	public void chainOfInducers_filters_inSameColumn() {
		TableQueryStep s1 =
				a.toBuilder().filter(ColumnFilter.matchIn("a", "a1")).groupBy(GroupByColumns.named("a")).build();
		TableQueryStep s2 =
				a.toBuilder().filter(ColumnFilter.matchIn("a", "a1", "a2")).groupBy(GroupByColumns.named("a")).build();
		TableQueryStep s3 = a.toBuilder()
				.filter(ColumnFilter.matchIn("a", "a1", "a2", "a3"))
				.groupBy(GroupByColumns.named("a"))
				.build();

		IAdhocDag<TableQueryStep> merged = GraphHelpers.makeGraph();
		merged.addVertex(s1);
		merged.addVertex(s2);
		merged.addVertex(s3);

		merged.addEdge(s1, s3);
		merged.addEdge(s2, s3);

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(merged);

		Assertions.assertThat(withShared.vertexSet()).hasSize(3).contains(s1, s2, s3);

		Assertions.assertThat(withShared.edgeSet())
				.hasSize(2)
				.anySatisfy(GraphsTestHelpers.assertEdge(s1, s3, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s2, s3, withShared));
	}

	/**
	 * {@code s2} has a broader filter than {@code s1} ({@code a==a1} vs {@code a==a1&b==b1}). When processing
	 * {@code s3}, the computed shared step equals {@code s2} — it is already an induced step and is the broadest of the
	 * two. Rather than inserting a new node, {@code s2} is promoted to shared-node role: {@code s1} is rewired through
	 * it, yielding {@code s1 → s2 → s3}.
	 */
	@Test
	public void chainOfInducers_filters_differentColumns() {
		TableQueryStep s1 = a.toBuilder().filter(AndFilter.and("a", "a1", "b", "b1")).build();
		TableQueryStep s2 =
				a.toBuilder().filter(AndFilter.and("a", "a1")).groupBy(GroupByColumns.named("a", "b")).build();
		TableQueryStep s3 =
				a.toBuilder().filter(AndFilter.and(Map.of())).groupBy(GroupByColumns.named("a", "b", "c")).build();

		IAdhocDag<TableQueryStep> merged = GraphHelpers.makeGraph();
		merged.addVertex(s1);
		merged.addVertex(s2);
		merged.addVertex(s3);

		merged.addEdge(s1, s3);
		merged.addEdge(s2, s3);

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(merged);

		// s2 is promoted to shared node: s1 is rewired through it; no new vertex is added
		Assertions.assertThat(withShared.vertexSet()).hasSize(3).contains(s1, s2, s3);

		Assertions.assertThat(withShared.edgeSet())
				.hasSize(2)
				.anySatisfy(GraphsTestHelpers.assertEdge(s1, s2, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s2, s3, withShared));
	}

	/**
	 * Motivating example for the topological-order initialisation in {@code AddSharedNodes.addSharedNodes}.
	 *
	 * <p>
	 * Three leaves under root: {@code A} and {@code B} share {@code country=FR}; {@code C} has {@code country=DE} and
	 * is unrelated. The algorithm must insert exactly one shared node for {@code {A, B}} and leave {@code C} pointing
	 * directly to root.
	 *
	 * <p>
	 * With <em>arbitrary</em> vertex ordering, root can appear in the processing queue before its leaves. Root is then
	 * re-queued after the shared node is inserted, and the three leaves ({@code A}, {@code B}, {@code C}) are processed
	 * as pointless skip-checks between root's first and second visits. Topological order (leaves first) avoids this
	 * interleaving: root's two visits happen consecutively with no wasted iterations in between.
	 */
	@Test
	public void sharedFilter_topologicalOrderAvoidsRedundantSkipChecks() {
		TableQueryStep stepA = a.toBuilder()
				.filter(AndFilter.and("country", "FR", "category", "X"))
				.groupBy(GroupByColumns.named("country", "category"))
				.build();
		TableQueryStep stepB = a.toBuilder()
				.filter(AndFilter.and("country", "FR", "category", "Y"))
				.groupBy(GroupByColumns.named("country", "category"))
				.build();
		TableQueryStep stepC = a.toBuilder()
				.filter(ColumnFilter.matchEq("country", "DE"))
				.groupBy(GroupByColumns.named("country"))
				.build();
		TableQueryStep root = a.toBuilder()
				.filter(AndFilter.and(Map.of()))
				.groupBy(GroupByColumns.named("country", "category"))
				.build();

		IAdhocDag<TableQueryStep> dag = GraphHelpers.makeGraph();
		dag.addVertex(root);
		Stream.of(stepA, stepB, stepC).forEach(s -> {
			dag.addVertex(s);
			dag.addEdge(s, root);
		});

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(dag);

		// stepA and stepB are consolidated under a shared node; stepC stays directly under root
		TableQueryStep sharedFR = a.toBuilder()
				.filter(AndFilter.and("country", "FR", "category", InMatcher.matchIn("X", "Y")))
				.groupBy(GroupByColumns.named("country", "category"))
				.build();

		Assertions.assertThat(withShared.vertexSet()).hasSize(5).containsAll(dag.vertexSet()).contains(sharedFR);

		Assertions.assertThat(withShared.edgeSet())
				.hasSize(4)
				.anySatisfy(GraphsTestHelpers.assertEdge(stepA, sharedFR, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(stepB, sharedFR, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(sharedFR, root, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(stepC, root, withShared));
	}

	/**
	 * Two-level input DAG: root has two intermediate nodes {@code P} and {@code Q}, each with two leaf children that
	 * share a filter part. Shared nodes must be created at both intermediate levels independently; root itself has
	 * nothing to share between {@code P} and {@code Q}.
	 *
	 * <p>
	 * This is the structural scenario described in the topological-order comment of
	 * {@code AddSharedNodes.addSharedNodes}: with leaves-first processing, each intermediate node's children are
	 * already evaluated before the intermediate node itself is visited, so shared-node insertions at the lower level
	 * are made without being interleaved with higher-level work.
	 */
	@Test
	public void sharedFilter_twoLevelDag_intermediateNodesConsolidateChildren() {
		// P ← { A(country=FR, type=web), B(country=FR, type=app) }
		// Q ← { C(country=DE, type=web), D(country=DE, type=app) }
		// root ← { P, Q }
		TableQueryStep stepA = a.toBuilder()
				.filter(AndFilter.and("country", "FR", "type", "web"))
				.groupBy(GroupByColumns.named("country", "type"))
				.build();
		TableQueryStep stepB = a.toBuilder()
				.filter(AndFilter.and("country", "FR", "type", "app"))
				.groupBy(GroupByColumns.named("country", "type"))
				.build();
		TableQueryStep stepC = a.toBuilder()
				.filter(AndFilter.and("country", "DE", "type", "web"))
				.groupBy(GroupByColumns.named("country", "type"))
				.build();
		TableQueryStep stepD = a.toBuilder()
				.filter(AndFilter.and("country", "DE", "type", "app"))
				.groupBy(GroupByColumns.named("country", "type"))
				.build();

		TableQueryStep nodeP = a.toBuilder()
				.filter(ColumnFilter.matchEq("country", "FR"))
				.groupBy(GroupByColumns.named("country", "type"))
				.build();
		TableQueryStep nodeQ = a.toBuilder()
				.filter(ColumnFilter.matchEq("country", "DE"))
				.groupBy(GroupByColumns.named("country", "type"))
				.build();
		TableQueryStep root =
				a.toBuilder().filter(AndFilter.and(Map.of())).groupBy(GroupByColumns.named("country", "type")).build();

		IAdhocDag<TableQueryStep> dag = GraphHelpers.makeGraph();
		dag.addVertex(root);
		dag.addVertex(nodeP);
		dag.addEdge(nodeP, root);
		dag.addVertex(nodeQ);
		dag.addEdge(nodeQ, root);
		Stream.of(stepA, stepB).forEach(s -> {
			dag.addVertex(s);
			dag.addEdge(s, nodeP);
		});
		Stream.of(stepC, stepD).forEach(s -> {
			dag.addVertex(s);
			dag.addEdge(s, nodeQ);
		});

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(dag);

		// One shared node under P for {A, B}, one under Q for {C, D}; root keeps {P, Q} unchanged
		TableQueryStep sharedFR = a.toBuilder()
				.filter(AndFilter.and("country", "FR", "type", InMatcher.matchIn("web", "app")))
				.groupBy(GroupByColumns.named("country", "type"))
				.build();
		TableQueryStep sharedDE = a.toBuilder()
				.filter(AndFilter.and("country", "DE", "type", InMatcher.matchIn("web", "app")))
				.groupBy(GroupByColumns.named("country", "type"))
				.build();

		Assertions.assertThat(withShared.vertexSet())
				.hasSize(dag.vertexSet().size() + 2)
				.containsAll(dag.vertexSet())
				.contains(sharedFR, sharedDE);

		Assertions.assertThat(withShared.edgeSet())
				.hasSize(dag.edgeSet().size() + 2)
				.anySatisfy(GraphsTestHelpers.assertEdge(stepA, sharedFR, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(stepB, sharedFR, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(sharedFR, nodeP, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(stepC, sharedDE, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(stepD, sharedDE, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(sharedDE, nodeQ, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(nodeP, root, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(nodeQ, root, withShared));
	}

	/**
	 * Three leaves under a single flat inducer, where two of the three form a sub-group:
	 * <ul>
	 * <li>{@code s1}, {@code s2}, {@code s3} all share {@code a==a1} → {@code sharedA} is created for all three;</li>
	 * <li>{@code s1} and {@code s2} additionally share {@code b==b1} → {@code sharedB} is then created under
	 * {@code sharedA} for those two.</li>
	 * </ul>
	 * The resulting chain is {@code s1 → sharedB → sharedA → inducer} (with {@code s2 → sharedB} and
	 * {@code s3 → sharedA}). {@code sharedB}'s direct inducer is {@code sharedA} — a node that was itself freshly
	 * inserted in the same pass. This verifies the recursive {@link AddSharedNodes#addSharedNode} call that eagerly
	 * stabilises each newly created shared node before returning.
	 */
	@Test
	public void sharedFilter_nestedSharedNodes_secondSharedNodeInducedByFirst() {
		TableQueryStep s1 =
				a.toBuilder().filter(AndFilter.and("a", "a1", "b", "b1")).groupBy(GroupByColumns.named("a")).build();
		TableQueryStep s2 =
				a.toBuilder().filter(AndFilter.and("a", "a1", "b", "b1")).groupBy(GroupByColumns.named("b")).build();
		TableQueryStep s3 =
				a.toBuilder().filter(AndFilter.and("a", "a1", "b", "b2")).groupBy(GroupByColumns.named("b")).build();
		TableQueryStep inducer =
				a.toBuilder().filter(AndFilter.and(Map.of())).groupBy(GroupByColumns.named("a", "b")).build();

		IAdhocDag<TableQueryStep> dag = GraphHelpers.makeGraph();
		dag.addVertex(inducer);
		Stream.of(s1, s2, s3).forEach(s -> {
			dag.addVertex(s);
			dag.addEdge(s, inducer);
		});

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(dag);

		// Groups all three: a==a1 is the widest common part
		TableQueryStep sharedA = a.toBuilder()
				.filter(AndFilter.and("a", "a1", "b", InMatcher.matchIn("b1", "b2")))
				.groupBy(GroupByColumns.named("a", "b"))
				.build();
		// Groups s1 and s2 only: both carry b==b1 on top of a==a1
		TableQueryStep sharedB = a.toBuilder()
				.filter(AndFilter.and("a", "a1", "b", "b1"))
				.groupBy(GroupByColumns.named("a", "b"))
				.build();

		Assertions.assertThat(withShared.vertexSet())
				.hasSize(dag.vertexSet().size() + 2)
				.containsAll(dag.vertexSet())
				.contains(sharedA, sharedB);

		Assertions.assertThat(withShared.edgeSet())
				.hasSize(5)
				.anySatisfy(GraphsTestHelpers.assertEdge(s1, sharedB, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s2, sharedB, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(sharedB, sharedA, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s3, sharedA, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(sharedA, inducer, withShared));
	}

	/**
	 * Two completely disjoint groups of leaves under the same flat inducer:
	 * <ul>
	 * <li>{@code s1} and {@code s2} share {@code a==a1};</li>
	 * <li>{@code s3} and {@code s4} share {@code b==b1}.</li>
	 * </ul>
	 * After the first call to {@link AddSharedNodes#tryInsertSharedNode} creates {@code sharedA} for {@code {s1, s2}},
	 * the inducer's induced set becomes {@code {sharedA, s3, s4}} — still size&nbsp;3 — so the {@code while} loop in
	 * {@link AddSharedNodes#addSharedNode} iterates a second time and creates {@code sharedB} for {@code {s3, s4}}.
	 * This is the minimal scenario (4 leaves + 1 inducer) that exercises two consecutive successful
	 * {@code tryInsertSharedNode} calls on the same node.
	 */
	@Test
	public void sharedFilter_twoDisjointGroups_inducesTwoConsecutiveSharedNodes() {
		TableQueryStep s1 = a.toBuilder()
				.filter(AndFilter.and("a", "a1", "c", "c1"))
				.groupBy(GroupByColumns.named("a", "c"))
				.build();
		TableQueryStep s2 = a.toBuilder()
				.filter(AndFilter.and("a", "a1", "c", "c2"))
				.groupBy(GroupByColumns.named("a", "c"))
				.build();
		TableQueryStep s3 = a.toBuilder()
				.filter(AndFilter.and("b", "b1", "d", "d1"))
				.groupBy(GroupByColumns.named("b", "d"))
				.build();
		TableQueryStep s4 = a.toBuilder()
				.filter(AndFilter.and("b", "b1", "d", "d2"))
				.groupBy(GroupByColumns.named("b", "d"))
				.build();
		TableQueryStep inducer =
				a.toBuilder().filter(AndFilter.and(Map.of())).groupBy(GroupByColumns.named("a", "b", "c", "d")).build();

		IAdhocDag<TableQueryStep> dag = GraphHelpers.makeGraph();
		dag.addVertex(inducer);
		Stream.of(s1, s2, s3, s4).forEach(s -> {
			dag.addVertex(s);
			dag.addEdge(s, inducer);
		});

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(dag);

		TableQueryStep sharedA = a.toBuilder()
				.filter(AndFilter.and("a", "a1", "c", InMatcher.matchIn("c1", "c2")))
				.groupBy(GroupByColumns.named("a", "c"))
				.build();
		TableQueryStep sharedB = a.toBuilder()
				.filter(AndFilter.and("b", "b1", "d", InMatcher.matchIn("d1", "d2")))
				.groupBy(GroupByColumns.named("b", "d"))
				.build();

		Assertions.assertThat(withShared.vertexSet())
				.hasSize(dag.vertexSet().size() + 2)
				.containsAll(dag.vertexSet())
				.contains(sharedA, sharedB);

		Assertions.assertThat(withShared.edgeSet())
				.hasSize(6)
				.anySatisfy(GraphsTestHelpers.assertEdge(s1, sharedA, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s2, sharedA, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(sharedA, inducer, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s3, sharedB, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s4, sharedB, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(sharedB, inducer, withShared));
	}

	/**
	 * The inducer {@code s4} carries a filter column ({@code c}) that none of the leaves reference. The shared-node
	 * computation must therefore strip {@code c} from the inducer's groupBy when building the intermediate nodes. The
	 * leaves form the same nested structure as {@link #sharedFilter_nestedSharedNodes_secondSharedNodeInducedByFirst}:
	 * {@code shared} groups all three leaves and {@code shared2} groups the two leaves that additionally share
	 * {@code b==b1}.
	 */
	@Test
	public void sharedFilter_parentHasIrrelevantFilterToChildren() {
		TableQueryStep s1 =
				a.toBuilder().filter(AndFilter.and("a", "a1", "b", "b1")).groupBy(GroupByColumns.named("a")).build();
		TableQueryStep s2 =
				a.toBuilder().filter(AndFilter.and("a", "a1", "b", "b1")).groupBy(GroupByColumns.named("b")).build();
		TableQueryStep s3 =
				a.toBuilder().filter(AndFilter.and("a", "a1", "b", "b2")).groupBy(GroupByColumns.named("b")).build();
		TableQueryStep s4 = a.toBuilder()
				.filter(FilterBuilder
						.or(ColumnFilter.matchEq("c", "c1"),
								AndFilter.and(ColumnFilter.matchEq("a", "a1"),
										ColumnFilter.matchIn("b", "b1", "b2", "b3")))
						.combine())
				.groupBy(GroupByColumns.named("a", "b", "c"))
				.build();

		IAdhocDag<TableQueryStep> merged = GraphHelpers.makeGraph();
		merged.addVertex(s1);
		merged.addVertex(s2);
		merged.addVertex(s3);
		merged.addVertex(s4);

		merged.addEdge(s1, s4);
		merged.addEdge(s2, s4);
		merged.addEdge(s3, s4);

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(merged);

		// Helps computing the 3 nodes
		TableQueryStep shared = a.toBuilder()
				.filter(AndFilter.and("a", "a1", "b", InMatcher.matchIn("b1", "b2")))
				.groupBy(GroupByColumns.named("a", "b"))
				.build();
		// Helps computing the 2 nodes on `b1`
		TableQueryStep shared2 = a.toBuilder()
				.filter(AndFilter.and("a", "a1", "b", "b1"))
				.groupBy(GroupByColumns.named("a", "b"))
				.build();
		Assertions.assertThat(withShared.vertexSet())
				.hasSize(merged.vertexSet().size() + 2)
				.containsAll(merged.vertexSet())
				.contains(shared)
				.contains(shared2);

		Assertions.assertThat(withShared.edgeSet())
				.hasSize(5)
				.anySatisfy(GraphsTestHelpers.assertEdge(s1, shared2, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s2, shared2, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(shared2, shared, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s3, shared, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(shared, s4, withShared));
	}

	/**
	 * Two fully independent subtrees (disconnected components) are given as a single input DAG. Each subtree has two
	 * leaves sharing a filter part, so each should produce one shared node. The multi-component path in
	 * {@link AddSharedNodes#addSharedNodes} dispatches them concurrently; the final result must contain both shared
	 * nodes as if they had been processed sequentially.
	 */
	@Test
	public void twoDisconnectedComponents_eachGetsItsOwnSharedNode() {
		// Component 1: inductorA ← { s1(country=FR,type=web), s2(country=FR,type=app) }
		TableQueryStep s1 = a.toBuilder()
				.filter(AndFilter.and("country", "FR", "type", "web"))
				.groupBy(GroupByColumns.named("country", "type"))
				.build();
		TableQueryStep s2 = a.toBuilder()
				.filter(AndFilter.and("country", "FR", "type", "app"))
				.groupBy(GroupByColumns.named("country", "type"))
				.build();
		TableQueryStep inductorA =
				a.toBuilder().filter(AndFilter.and(Map.of())).groupBy(GroupByColumns.named("country", "type")).build();

		// Component 2: inductorB ← { s3(region=EMEA,channel=online), s4(region=EMEA,channel=offline) }
		TableQueryStep s3 = a.toBuilder()
				.filter(AndFilter.and("region", "EMEA", "channel", "online"))
				.groupBy(GroupByColumns.named("region", "channel"))
				.build();
		TableQueryStep s4 = a.toBuilder()
				.filter(AndFilter.and("region", "EMEA", "channel", "offline"))
				.groupBy(GroupByColumns.named("region", "channel"))
				.build();
		TableQueryStep inductorB = a.toBuilder()
				.filter(AndFilter.and(Map.of()))
				.groupBy(GroupByColumns.named("region", "channel"))
				.build();

		IAdhocDag<TableQueryStep> dag = GraphHelpers.makeGraph();
		dag.addVertex(inductorA);
		Stream.of(s1, s2).forEach(s -> {
			dag.addVertex(s);
			dag.addEdge(s, inductorA);
		});
		dag.addVertex(inductorB);
		Stream.of(s3, s4).forEach(s -> {
			dag.addVertex(s);
			dag.addEdge(s, inductorB);
		});

		IAdhocDag<TableQueryStep> withShared = sharedNodes.addSharedNodes(dag);

		TableQueryStep sharedFR = a.toBuilder()
				.filter(AndFilter.and("country", "FR", "type", InMatcher.matchIn("web", "app")))
				.groupBy(GroupByColumns.named("country", "type"))
				.build();
		TableQueryStep sharedEMEA = a.toBuilder()
				.filter(AndFilter.and("region", "EMEA", "channel", InMatcher.matchIn("online", "offline")))
				.groupBy(GroupByColumns.named("region", "channel"))
				.build();

		Assertions.assertThat(withShared.vertexSet())
				.hasSize(dag.vertexSet().size() + 2)
				.containsAll(dag.vertexSet())
				.contains(sharedFR, sharedEMEA);

		Assertions.assertThat(withShared.edgeSet())
				.hasSize(6)
				.anySatisfy(GraphsTestHelpers.assertEdge(s1, sharedFR, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s2, sharedFR, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(sharedFR, inductorA, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s3, sharedEMEA, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(s4, sharedEMEA, withShared))
				.anySatisfy(GraphsTestHelpers.assertEdge(sharedEMEA, inductorB, withShared));
	}
}
