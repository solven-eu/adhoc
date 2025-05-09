package eu.solven.adhoc.engine;

import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.engine.step.CubeQueryStep;

public class TestTopologicalOrderSpliterator {
	@Test
	public void testTopologicalOrderSpliterator_empty() {
		DirectedAcyclicGraph<CubeQueryStep, DefaultEdge> dag =
				DirectedAcyclicGraph.<CubeQueryStep, DefaultEdge>createBuilder(DefaultEdge.class).build();
		Stream<CubeQueryStep> spliterator = TopologicalOrderSpliterator.fromDAG(dag);

		Assertions.assertThat(spliterator.isParallel()).isFalse();
	}
}
