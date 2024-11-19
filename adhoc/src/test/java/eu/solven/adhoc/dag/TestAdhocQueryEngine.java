package eu.solven.adhoc.dag;

import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.transformers.Aggregator;
import org.assertj.core.api.Assertions;
import org.greenrobot.eventbus.EventBus;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class TestAdhocQueryEngine {
    AdhocMeasureBag amg = AdhocMeasureBag.builder().build();
    AdhocQueryEngine aqe = AdhocQueryEngine.builder().measureBag(amg).eventBus(new EventBus()).build();

    @Test
    public void testColumnToAggregationKeys() {
        amg.addMeasure(Aggregator.builder().name("n1").columnName("c1").aggregationKey("A").build());
        amg.addMeasure(Aggregator.builder().name("n2").columnName("c1").aggregationKey("B").build());
        amg.addMeasure(Aggregator.builder().name("n3").aggregationKey("C").build());
        amg.addMeasure(Aggregator.builder().name("n4").build());


        IAdhocQuery adhocQuery = AdhocQuery.builder().measures(amg.getNameToMeasure().keySet()).build();
        DirectedAcyclicGraph<AdhocQueryStep, DefaultEdge> fromQueriedToAggregates = aqe.makeQueryStepsDag(adhocQuery);
        Map<String, Set<Aggregator>> columnToAggregators = aqe.columnToAggregators(fromQueriedToAggregates);

        Assertions.assertThat(columnToAggregators).hasSize(3)
                .containsEntry("c1", Set.of((Aggregator) amg.getNameToMeasure().get("n1"), (Aggregator) amg.getNameToMeasure().get("n2")))
                .containsEntry("n3", Set.of((Aggregator) amg.getNameToMeasure().get("n3")))
                .containsEntry("n4", Set.of((Aggregator) amg.getNameToMeasure().get("n4")));
    }
}
