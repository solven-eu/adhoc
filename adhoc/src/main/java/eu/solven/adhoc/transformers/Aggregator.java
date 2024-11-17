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
	// The name/identifier of the measure
	@NonNull
	String name;

	// The name of the underlying aggregated column
	String columnName;

	@NonNull
	@Default
	String aggregationKey = SumAggregator.KEY;

	public String getColumnName() {
		if (columnName != null) {
			return columnName;
		} else {
			// The default columnName is the aggregator name
			return name;
		}
	}
}
