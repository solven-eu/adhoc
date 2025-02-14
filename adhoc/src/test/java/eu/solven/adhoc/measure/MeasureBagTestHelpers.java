package eu.solven.adhoc.measure;

import eu.solven.adhoc.measure.step.Aggregator;
import eu.solven.adhoc.measure.step.IHasUnderlyingMeasures;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.extern.slf4j.Slf4j;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import java.util.Map;

@Slf4j
public class MeasureBagTestHelpers {

    /**
     * This is the DAG of measure. It is a simplistic view of the measures graph, as it may not reflect the impacts of
     * {@link IMeasure} requesting underlying measures with custom {@link IAdhocFilter} or {@link IAdhocGroupBy}.
     *
     * @return
     */
    public static DirectedAcyclicGraph<IMeasure, DefaultEdge> makeMeasuresDag(IAdhocMeasureBag measureBag) {
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
