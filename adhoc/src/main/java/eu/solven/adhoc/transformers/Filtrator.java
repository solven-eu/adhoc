package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.dag.AdhocQueryStep;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * A Filtrator is an {@link IMeasure} which is filtering another {@link IMeasure} given a {@link IAdhocFilter}. The
 * input {@link IAdhocFilter} will be `AND`-ed with the {@link AdhocQueryStep} own {@link IAdhocFilter}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Slf4j
public class Filtrator implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;
	
	@Singular
	Set<String> tags;

	@NonNull
	String underlying;

	@NonNull
	IAdhocFilter filter;

	@Override
	public List<String> getUnderlyingNames() {
		return Collections.singletonList(underlying);
	}

	@Override
	public IHasUnderlyingQuerySteps wrapNode(IOperatorsFactory transformationFactory, AdhocQueryStep step) {
		return new FiltratorQueryStep(this, transformationFactory, step);
	}

}
