package eu.solven.adhoc.dag;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.IHasUnderlyingMeasures;
import eu.solven.adhoc.transformers.IMeasure;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * The set of named {@link IMeasure}. Many of them are {@link IHasUnderlyingMeasures}, which express a
 * Directed-Acyclic-Graph. The DAG is actually evaluated on a per-query basis.
 * 
 * @author Benoit Lacelle
 *
 */
@RequiredArgsConstructor
@Slf4j
@Builder
public class AdhocMeasuresSet {

	@Default
	final Map<String, IMeasure> nameToMeasure = new ConcurrentHashMap<>();

	public AdhocMeasuresSet addMeasure(IMeasure namedMeasure) {
		String name = namedMeasure.getName();

		if (nameToMeasure.containsKey(name)) {
			throw new IllegalArgumentException(
					"Can not replace a measure in `.addMeasure`, Conflicting name is %s".formatted(name));
		}

		nameToMeasure.put(name, namedMeasure);

		return this;
	}

	public IMeasure resolveIfRef(IMeasure measure) {
		if (measure instanceof ReferencedMeasure ref) {
			String refName = ref.getRef();
			IMeasure resolved = nameToMeasure.get(refName);

			if (resolved == null) {
				throw new IllegalArgumentException("No measure named: %s".formatted(refName));
			}

			return resolved;
		}
		return measure;
	}

	/**
	 * This is the DAG of measure. It is a simplistic view of the measures graph, as it may not reflect the impacts of
	 * {@link IMeasure} requesting underlying measures with custom {@link IAdhocFilter} or {@link IAdhocGroupBy}.
	 * 
	 * @param adhocQuery
	 * @return
	 */
	public DirectedAcyclicGraph<IMeasure, DefaultEdge> makeMeasuresDag() {
		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = new DirectedAcyclicGraph<>(DefaultEdge.class);

		nameToMeasure.forEach((name, measure) -> {
			measuresDag.addVertex(measure);

			if (measure instanceof Aggregator aggregator) {
				log.debug("Aggregators (here {}) do not have any underlying measure", aggregator);
			} else if (measure instanceof IHasUnderlyingMeasures combinator) {
				for (String underlyingName : combinator.getUnderlyingNames()) {
					// Make sure the DAG has actual measure nodes, and not references
					IMeasure notRefMeasure = nameToMeasure.get(underlyingName);

					if (notRefMeasure == null) {
						throw new IllegalArgumentException("`%s` references as unknown measure: `%s`"
								.formatted(measure.getName(), underlyingName));
					}

					measuresDag.addVertex(notRefMeasure);
					measuresDag.addEdge(measure, notRefMeasure);
				}
			} else {
				throw new UnsupportedOperationException(PepperLogHelper.getObjectAndClass(measure).toString());
			}
		});

		return measuresDag;
	}

	// TODO Why doesn't this compile?
	// public static class AdhocMeasuresSetBuilder {
	// public AdhocMeasuresSetBuilder measure(IMeasure measure) {
	// this.nameToMeasure.put(measure.getName(), measure);
	//
	// return this;
	// }
	// }

}
