package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.aggregations.sum.SumCombination;
import eu.solven.adhoc.dag.AdhocQueryStep;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Combinator} is a {@link IMeasure} which combines the underlying measures for current coordinate.
 */
@Value
@Builder
@Slf4j
public class Combinator implements IMeasure, IHasUnderlyingMeasures, IHasCombinationKey {
	@NonNull
	String name;

	@Singular
	Set<String> tags;

	@NonNull
	List<String> underlyingNames;

	 /**
	 * @see eu.solven.adhoc.aggregations.ICombination
	 */
	@NonNull
	@Default
	String combinationKey = SumCombination.KEY;

	 /**
	 * @see eu.solven.adhoc.aggregations.ICombination
	 */
	@NonNull
	@Default
	Map<String, ?> combinationOptions = Collections.emptyMap();

	@Override
	public Map<String, ?> getCombinationOptions() {
		return makeAllOptions(this, combinationOptions);
	}

	public static Map<String, ?> makeAllOptions(IHasUnderlyingMeasures hasUnderlyings, Map<String, ?> explicitOptions) {
		Map<String, Object> allOptions = new HashMap<>();

		// Default options
		allOptions.put("underlyingNames", hasUnderlyings.getUnderlyingNames());

		// override with explicit options
		allOptions.putAll(explicitOptions);

		return allOptions;
	}

	public static CombinatorBuilder forceBuilder() {
		return Combinator.builder();
	}

	@Override
	public IHasUnderlyingQuerySteps wrapNode(IOperatorsFactory transformationFactory, AdhocQueryStep step) {
		return new CombinatorQueryStep(this, transformationFactory, step);
	}

}
