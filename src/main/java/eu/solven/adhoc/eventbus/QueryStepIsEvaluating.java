package eu.solven.adhoc.eventbus;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import lombok.Builder;
import lombok.Value;

/**
 * We start evaluating a queryStep, given underlying measures {@link CoordinatesToValues}. Once done, we'll have
 * computed a {@link CoordinatesToValues} for current {@link AdhocQueryStep}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class QueryStepIsEvaluating {
	AdhocQueryStep queryStep;
}
