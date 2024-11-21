package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.aggregations.sum.SumCombination;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.dag.AdhocQueryStep;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * This {@link IMeasure} will aggregate underlying measure, evaluated at buckets defined by a {@link IAdhocGroupBy}, and
 * aggregated through an {@link IAggregation}.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@Slf4j
public class Bucketor implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;
	
	@Singular
	Set<String> tags;

	@NonNull
	@Singular
	List<String> underlyingNames;

	@NonNull
	@Default
	String aggregationKey = SumAggregator.KEY;

	// Accept a combinator key, to be applied on each groupBy
	@NonNull
	@Default
	String combinatorKey = SumCombination.KEY;

	@NonNull
	@Default
	Map<String, ?> combinatorOptions = Collections.emptyMap();

	@NonNull
	@Default
	IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;

	@Override
	public List<String> getUnderlyingNames() {
		return underlyingNames;
	}
	
	@Override
	public IHasUnderlyingQuerySteps wrapNode(IOperatorsFactory transformationFactory,
			AdhocQueryStep queryStep) {
		return new BucketorQueryStep(this, transformationFactory, queryStep);
	}


}
