package eu.solven.adhoc;

import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.transformers.Aggregator;

public interface IAdhocTestConstants {
	Aggregator k1Sum = Aggregator.builder().name("k1").aggregationKey(SumAggregator.KEY).build();
	Aggregator k2Sum = Aggregator.builder().name("k2").aggregationKey(SumAggregator.KEY).build();
}
