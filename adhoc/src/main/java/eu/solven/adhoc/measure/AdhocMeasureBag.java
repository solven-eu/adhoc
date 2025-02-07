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
package eu.solven.adhoc.measure;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import eu.solven.adhoc.measure.step.Aggregator;
import eu.solven.adhoc.measure.step.IHasUnderlyingMeasures;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.resource.MeasuresSetFromResource;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * The set of named {@link IMeasure}. Many of them are {@link IHasUnderlyingMeasures}, which express a
 * Directed-Acyclic-Graph. The DAG is actually evaluated on a per-query basis.
 * 
 * This may not generate a single-connected DAG, as it may contains unfinished chains of measures.
 * 
 * @author Benoit Lacelle
 *
 */
@RequiredArgsConstructor
@Slf4j
@Builder
public class AdhocMeasureBag implements IAdhocMeasureBag {
	@Getter
	final String name;

	@Default
	@Getter
	final Map<String, IMeasure> nameToMeasure = new ConcurrentHashMap<>();

	public AdhocMeasureBag addMeasure(IMeasure namedMeasure) {
		String measureName = namedMeasure.getName();

		if (nameToMeasure.containsKey(measureName)) {
			throw new IllegalArgumentException(
					"bag=%s Can not replace a measure in `.addMeasure`, Conflicting name is %s".formatted(name,
							measureName));
		}

		nameToMeasure.put(measureName, namedMeasure);

		return this;
	}

	@Override
	public IMeasure resolveIfRef(IMeasure measure) {
		if (measure instanceof ReferencedMeasure ref) {
			String refName = ref.getRef();
			IMeasure resolved = nameToMeasure.get(refName);

			if (resolved == null) {
				String minimizing = MeasuresSetFromResource.minimizingDistance(getNameToMeasure().keySet(), refName);

				throw new IllegalArgumentException(
						"bag=%s No measure named: %s. Did you meant: %s".formatted(name, refName, minimizing));
			}

			return resolved;
		}
		return measure;
	}

	@Override
	public Optional<IMeasure> resolveIfRefOpt(IMeasure measure) {
		if (measure instanceof ReferencedMeasure ref) {
			String refName = ref.getRef();
			IMeasure resolved = nameToMeasure.get(refName);

			return Optional.ofNullable(resolved);
		}
		return Optional.of(measure);
	}

	/**
	 * This is the DAG of measure. It is a simplistic view of the measures graph, as it may not reflect the impacts of
	 * {@link IMeasure} requesting underlying measures with custom {@link IAdhocFilter} or {@link IAdhocGroupBy}.
	 * 
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
						throw new IllegalArgumentException("`%s` references an unknown measure: `%s`"
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

	public static AdhocMeasureBag fromMeasures(List<IMeasure> measures) {
		AdhocMeasureBag ams = AdhocMeasureBag.builder().build();

		measures.forEach(ams::addMeasure);

		return ams;
	}

	public AdhocMeasureBag acceptMeasureCombinator(IMeasureBagVisitor asCombinator) {
		return asCombinator.addMeasures(this);
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
