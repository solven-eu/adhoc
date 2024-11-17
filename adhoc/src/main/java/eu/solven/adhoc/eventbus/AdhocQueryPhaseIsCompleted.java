package eu.solven.adhoc.eventbus;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class AdhocQueryPhaseIsCompleted {
	String phase;
}
