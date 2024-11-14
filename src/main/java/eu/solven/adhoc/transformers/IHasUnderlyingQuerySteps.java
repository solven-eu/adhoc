package eu.solven.adhoc.transformers;

import java.util.List;

import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;

public interface IHasUnderlyingQuerySteps {
	List<AdhocQueryStep> getUnderlyingSteps();

	CoordinatesToValues produceOutputColumn(List<CoordinatesToValues> underlyings);
}
