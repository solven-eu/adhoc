package eu.solven.adhoc.eventbus;

import eu.solven.adhoc.transformers.IMeasure;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class MeasuratorIsCompleted {
	IMeasure measurator;
	long nbCells;
}
