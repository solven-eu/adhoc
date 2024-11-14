package eu.solven.adhoc.aggregations;

import java.util.List;
import java.util.Map;

import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.dag.AdhocQueryStep;

public interface IDecomposition {
	Map<Map<String, ?>, Object> decompose(Map<String, ?> coordinate, Object value);

	/**
	 * 
	 * @param step
	 * @return the columns which MAY be written by decompositions.
	 */
	List<IWhereGroupbyAdhocQuery> getUnderlyingSteps(AdhocQueryStep step);
}
