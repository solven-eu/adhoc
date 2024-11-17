package eu.solven.adhoc.dag;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import eu.solven.adhoc.api.v1.IAdhocQuery;
import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.transformers.IMeasure;
import lombok.Builder;
import lombok.Value;

/**
 * Given an {@link IAdhocQuery} and a {@link AdhocMeasuresSet}, we need to compute each underlying measure at a given
 * {@link IWhereGroupbyAdhocQuery}.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
public class AdhocQueryStep implements IWhereGroupbyAdhocQuery {
	IAdhocFilter filter;
	IAdhocGroupBy groupBy;

	IMeasure measure;

	public static AdhocQueryStepBuilder edit(AdhocQueryStep step) {
		return edit((IWhereGroupbyAdhocQuery) step).measure(step.getMeasure());
	}

	public static AdhocQueryStepBuilder edit(IWhereGroupbyAdhocQuery step) {
		return AdhocQueryStep.builder().filter(step.getFilter()).groupBy(step.getGroupBy());
	}

}
