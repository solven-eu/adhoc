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
package eu.solven.adhoc.measure;

import java.util.Map;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MeasureBagTestHelpers {

	/**
	 * This is the DAG of measure. It is a simplistic view of the measures graph, as it may not reflect the impacts of
	 * {@link IMeasure} requesting underlying measures with custom {@link ISliceFilter} or {@link IAdhocGroupBy}.
	 *
	 * @return
	 */
	public static DirectedAcyclicGraph<IMeasure, DefaultEdge> makeMeasuresDag(IMeasureForest measureBag) {
		DirectedAcyclicGraph<IMeasure, DefaultEdge> measuresDag = new DirectedAcyclicGraph<>(DefaultEdge.class);

		Map<String, IMeasure> nameToMeasure = measureBag.getNameToMeasure();
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
				throw new UnsupportedOperationException(
						"Unsupported %s".formatted(PepperLogHelper.getObjectAndClass(measure)));
			}
		});

		return measuresDag;
	}
}
