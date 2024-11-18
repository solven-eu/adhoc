package eu.solven.adhoc.transformers;

import java.util.Set;

import eu.solven.adhoc.aggregations.sum.SumAggregator;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
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
	
	@Singular
	Set<String> tags;

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
