package eu.solven.adhoc.transformers;

import eu.solven.adhoc.aggregations.SumAggregator;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;

/**
 * Used to transform an input column into a measure.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class Aggregator implements IMeasure {
	@NonNull
	String name;

	@NonNull
	@Default
	String aggregationKey = SumAggregator.KEY;
}
