package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.solven.adhoc.aggregations.ICombination;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.aggregations.sum.SumCombination;
import eu.solven.adhoc.dag.AdhocQueryStep;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder
@Slf4j
public class Combinator implements IMeasure, IHasUnderlyingMeasures, IHasCombinationKey {
	@NonNull
	String name;

	@Default
	boolean debug = false;

	@NonNull
	List<String> underlyingNames;

	/**
	 * @see ICombination
	 */
	@NonNull
	@Default
	String combinationKey = SumCombination.KEY;

	/**
	 * @see ICombination
	 */
	@NonNull
	@Default
	Map<String, ?> options = Collections.emptyMap();

	@Override
	public Map<String, ?> getCombinationOptions() {
		return makeAllOptions(this, options);
	}

	public static Map<String, ?> makeAllOptions(IHasUnderlyingMeasures hasUnderlyings, Map<String, ?> explicitOptions) {
		Map<String, Object> allOptions = new HashMap<>();

		// Default options
		allOptions.put("underlyingNames", hasUnderlyings.getUnderlyingNames());

		// override with explicit options
		allOptions.putAll(explicitOptions);

		return allOptions;
	}

	@Override
	public IHasUnderlyingQuerySteps wrapNode(IOperatorsFactory transformationFactory, AdhocQueryStep step) {
		return new CombinatorQueryStep(this, transformationFactory, step);
	}

}
