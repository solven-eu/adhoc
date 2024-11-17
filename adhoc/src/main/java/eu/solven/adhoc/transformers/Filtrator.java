package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.List;

import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.dag.AdhocQueryStep;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder
@Slf4j
public class Filtrator implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;

	@Default
	boolean debug = false;

	@NonNull
	String underlyingMeasure;

	@NonNull
	IAdhocFilter filter;

	@Override
	public List<String> getUnderlyingNames() {
		return Collections.singletonList(underlyingMeasure);
	}
	
	@Override
	public IHasUnderlyingQuerySteps wrapNode(IOperatorsFactory transformationFactory,
			AdhocQueryStep step) {
		return new FiltratorQueryStep(this, transformationFactory, step);
	}

}
