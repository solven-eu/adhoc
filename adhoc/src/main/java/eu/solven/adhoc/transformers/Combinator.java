package eu.solven.adhoc.transformers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.aggregations.sum.SumCombination;
import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.pojo.AndFilter;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.GroupByColumns;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Combinator} is a {@link IMeasure} which combines the underlying measures for current coordinate.
 */
@Value
@Builder
@Jacksonized
@Slf4j
public class Combinator implements IMeasure, IHasUnderlyingMeasures, IHasCombinationKey {
	@NonNull
	String name;

	@NonNull
	@Singular
	Set<String> tags;

	@NonNull
	@Singular
	List<String> underlyings;

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
	public List<String> getUnderlyingNames() {
		return underlyings;
	}

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

	@Override
	public IHasUnderlyingQuerySteps wrapNode(IOperatorsFactory transformationFactory, AdhocQueryStep step) {
		return new CombinatorQueryStep(this, transformationFactory, step);
	}

}
