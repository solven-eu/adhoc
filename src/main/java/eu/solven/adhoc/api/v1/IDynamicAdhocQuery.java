package eu.solven.adhoc.api.v1;

import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import eu.solven.adhoc.transformers.ReferencedMeasure;

/**
 * A aggregation query. It is configured by:
 * 
 * - a filtering condition - axes along which the result is sliced - aggregations accumulating some measures
 * 
 * @author Benoit Lacelle
 *
 */
@Deprecated(since = "Unclear use of Lambda for properties")
public interface IDynamicAdhocQuery extends IWhereGroupbyAdhocQuery, IHasRefMeasures {

	/**
	 * 
	 * @param hasAggregations
	 * @return a new {@link IAdhocQuery} based on input {@link IHasRefMeasures}
	 */
	IDynamicAdhocQuery addAggregations(IHasRefMeasures hasAggregations);

	default IDynamicAdhocQuery addAggregations(ReferencedMeasure first, ReferencedMeasure... rest) {
		return addAggregations(() -> Lists.asList(first, rest).stream().collect(Collectors.toSet()));
	}
}