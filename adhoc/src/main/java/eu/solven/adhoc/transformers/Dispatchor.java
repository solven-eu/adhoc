package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.solven.adhoc.aggregations.AdhocIdentity;
import eu.solven.adhoc.aggregations.IAggregation;
import eu.solven.adhoc.aggregations.IDecomposition;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.dag.AdhocQueryStep;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * This {@link IMeasure} will aggregate underlying measure into new coordinates. If multiple input coordinates write in
 * the same output coordinates, the values are aggregated with the configured aggregationKey.
 * 
 * A typical useCase is to generate a calculated column (e.g. a column which is the first letter of some underlying
 * column), or weigthed dispatching (e.g. input values with a column ranging any float between 0 and 1 should output a
 * column with either 0 or 1).
 * 
 * @author Benoit Lacelle
 * @see SingleBucketor
 */
@Value
@Builder
@Slf4j
public class Dispatchor implements IMeasure, IHasUnderlyingMeasures {
	@NonNull
	String name;
	
	@Singular
	Set<String> tags;

	@Default
	boolean debug = false;

	// A dispatcher has a single underlying measure, else it would be unclear how/when underlying measures should be
	// combined
	@NonNull
	String underlyingMeasure;

	/**
	 * @see IAggregation
	 */
	@NonNull
	@Default
	String aggregationKey = SumAggregator.KEY;

	/**
	 * @see IDecomposition
	 */
	@NonNull
	@Default
	String decompositionKey = AdhocIdentity.KEY;

	/**
	 * @see IDecomposition
	 */
	@NonNull
	@Default
	Map<String, ?> decompositionOptions = Collections.emptyMap();

	// // Accept a combinator key, to be applied on each groupBy
	// @NonNull
	// @Default
	// String combinatorKey = SumTransformation.KEY;
	//
	// @NonNull
	// @Default
	// Map<String, ?> combinatorOptions = Collections.emptyMap();

	// @NonNull
	// @Default
	// IAdhocGroupBy groupBy = IAdhocGroupBy.GRAND_TOTAL;

	@Override
	public List<String> getUnderlyingNames() {
		return Collections.singletonList(underlyingMeasure);
	}

	@Override
	public IHasUnderlyingQuerySteps wrapNode(IOperatorsFactory transformationFactory,
			AdhocQueryStep adhocSubQuery) {
		return new DispatchorQueryStep(this, transformationFactory, adhocSubQuery);
	}

}
