package eu.solven.adhoc.aggregations;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.solven.adhoc.api.v1.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.dag.AdhocQueryStep;

public class AdhocIdentity implements IDecomposition {
	public static final String KEY = "identity";

	@Override
	public Map<Map<String, ?>, Object> decompose(Map<String, ?> coordinate, Object value) {
		return Collections.singletonMap(Map.of(), value);
	}

	@Override
	public List<IWhereGroupbyAdhocQuery> getUnderlyingSteps(AdhocQueryStep step) {
		return Collections.singletonList(step);
	}

}
