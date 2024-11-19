package eu.solven.adhoc.dag;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.transformers.IMeasure;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Given an {@link IAdhocQuery} and a {@link AdhocMeasureBag}, we need to compute each underlying measure at a given
 * {@link IWhereGroupbyAdhocQuery}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class AdhocQueryStep implements IWhereGroupbyAdhocQuery {
	@NonNull
	IMeasure measure;
	@NonNull
	IAdhocFilter filter;
	@NonNull
	IAdhocGroupBy groupBy;

	// This property is transported down to the DatabaseQuery
	Object custom;

	public static AdhocQueryStepBuilder edit(AdhocQueryStep step) {
		return edit((IWhereGroupbyAdhocQuery) step).measure(step.getMeasure());
	}

	public static AdhocQueryStepBuilder edit(IWhereGroupbyAdhocQuery step) {
		return AdhocQueryStep.builder().filter(step.getFilter()).groupBy(step.getGroupBy());
	}

}
