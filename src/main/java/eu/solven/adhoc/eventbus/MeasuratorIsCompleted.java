package eu.solven.adhoc.eventbus;

import eu.solven.adhoc.transformers.IMeasurator;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class MeasuratorIsCompleted {
	IMeasurator measurator;
	long nbCells;
}
