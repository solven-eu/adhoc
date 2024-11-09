package eu.solven.adhoc.dag;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.IMeasure;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DAGForQuery {

	DirectedAcyclicGraph<IMeasure, DefaultEdge> directedGraph;
	Set<String> queriedMeasures;

	// Aggregators are applied to the input Stream
	// e.g. we SUM click_counts per visit per page up to the page granularity
	// Set<Aggregator> aggregators;

	// Each combinators is applied over aggregators and other combinators
	// Map<Combinator, List<IMeasurator>> derivedToUnderlying;

	// public Map<IMeasurator, List<IMeasurator>> getUnderlyingToDerived() {
	// Map<IMeasurator, List<IMeasurator>> underlyingToDerived = new HashMap<>();
	//
	// derivedToUnderlying.forEach((derived, underlyings) -> {
	// underlyings.forEach(underlying -> {
	// underlyingToDerived
	// .merge(underlying, List.of(derived), (l, r) -> Lists.newArrayList(Iterables.concat(l, r)));
	// });
	// });
	//
	// return underlyingToDerived;
	// }

	/**
	 * 
	 * @return the aggregators requested explicitly or implicitly.
	 */
	public Set<Aggregator> getImpliedAggregators() {
		Set<Aggregator> aggregators = new LinkedHashSet<Aggregator>();

		getBreadthFirst().forEachRemaining(measurator -> {
			if (measurator instanceof Aggregator aggregator) {
				aggregators.add(aggregator);
			}
		});

		return aggregators;
	}

	public BreadthFirstIterator<IMeasure, DefaultEdge> getBreadthFirst() {
		Set<IMeasure> queryMeasurators = getQueryMeasurators();

		if (queryMeasurators.size() != queriedMeasures.size()) {
			throw new IllegalStateException("Some measures are unknown amongst " + queriedMeasures);
		}

		return new BreadthFirstIterator<IMeasure, DefaultEdge>(directedGraph, queryMeasurators);
	}

	/**
	 * @return the {@link IMeasure} explicitly requested.
	 */
	public Set<IMeasure> getQueryMeasurators() {
		return directedGraph.vertexSet()
				.stream()
				.filter(m -> queriedMeasures.contains(m.getName()))
				.collect(Collectors.toSet());

	}
}
