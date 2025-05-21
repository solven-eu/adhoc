package eu.solven.adhoc.atoti.table;

import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import org.junit.jupiter.api.Test;

public class TestLoggingAtotiTable  {
    @Test
    public void testNew() {
        LoggingAtotiTable table = LoggingAtotiTable.builder().pivotId("someCubeName").build();

        table.streamSlices(QueryPod.forTable(table), TableQueryV2.builder().aggregator(FilteredAggregator.builder().aggregator(Aggregator.sum("v")).build()).build());
    }
}
