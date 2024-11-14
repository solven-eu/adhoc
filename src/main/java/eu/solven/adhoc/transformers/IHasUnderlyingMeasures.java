package eu.solven.adhoc.transformers;

import java.util.List;

import eu.solven.adhoc.aggregations.ITransformationFactory;
import eu.solven.adhoc.dag.AdhocQueryStep;

public interface IHasUnderlyingMeasures {
	List<String> getUnderlyingNames();

	IHasUnderlyingQuerySteps wrapNode(ITransformationFactory transformationFactory, AdhocQueryStep adhocSubQuery);
}
