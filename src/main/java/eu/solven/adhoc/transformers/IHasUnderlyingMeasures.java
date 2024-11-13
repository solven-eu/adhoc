package eu.solven.adhoc.transformers;

import java.util.List;

import eu.solven.adhoc.aggregations.ITransformationFactory;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;

public interface IHasUnderlyingMeasures {
	List<String> getUnderlyingMeasures();

	List<AdhocQueryStep> getUnderlyingSteps(AdhocQueryStep adhocSubQuery);

	CoordinatesToValues produceOutputColumn(ITransformationFactory transformationFactory,
			AdhocQueryStep queryStep,
			List<CoordinatesToValues> underlyings);
}
