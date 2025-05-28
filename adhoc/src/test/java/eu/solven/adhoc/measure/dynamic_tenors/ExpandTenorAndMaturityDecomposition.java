package eu.solven.adhoc.measure.dynamic_tenors;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.IDecompositionEntry;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.value.IValueMatcher;

public class ExpandTenorAndMaturityDecomposition implements IDecomposition, IExamplePnLExplainConstant {

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		return switch (column) {
		case K_TENOR:
		case K_MATURITY: {
			yield CoordinatesSample.builder()
					.estimatedCardinality(5)
					.coordinates(Arrays.asList("1M", "3M", "6M", "1Y", "2Y"))
					.build();
		}
		default:
			throw new IllegalArgumentException("Unexpected value: " + column);
		};
	}

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		return Map.of(K_TENOR, String.class, K_MATURITY, String.class);
	}

	@Override
	public List<IDecompositionEntry> decompose(ISliceWithStep slice, Object value) {
		if (!(value instanceof Map map)) {
			return List.of(IDecompositionEntry.of(Map.of(), value));
		}

		IValueMatcher tenorMatcher = FilterHelpers.getValueMatcher(slice.asFilter(), K_TENOR);
		IValueMatcher maturityMatcher = FilterHelpers.getValueMatcher(slice.asFilter(), K_MATURITY);

		Object tenor = map.get(K_TENOR);
		Object maturity = map.get(K_MATURITY);
		if (!tenorMatcher.match(tenor)) {
			return List.of();
		} else if (!maturityMatcher.match(maturity)) {
			return List.of();
		}

		boolean isTenorGroupedBy = slice.getColumns().contains(K_TENOR);
		boolean isMaturityGroupedBy = slice.getColumns().contains(K_MATURITY);

		if (isTenorGroupedBy) {
			if (isMaturityGroupedBy) {
				// grouped by tenor and maturity
				return List.of(IDecompositionEntry.of(Map.of(K_TENOR, tenor, K_MATURITY, maturity), value));
			} else {
				// grouped by tenor
				return List.of(IDecompositionEntry.of(Map.of(K_TENOR, tenor), value));
			}
		} else if (isMaturityGroupedBy) {
			// grouped by maturity
			return List.of(IDecompositionEntry.of(Map.of(K_MATURITY, maturity), value));
		} else {
			// none are grouped by
			return List.of(IDecompositionEntry.of(Map.of(), value));
		}
	}

	@Override
	public List<IWhereGroupByQuery> getUnderlyingSteps(CubeQueryStep step) {
		// Supress tenor and maturity as they are columns embedded in the underlying aggregate
		return List.of(IDecomposition.suppressColumn(IDecomposition.suppressColumn(step, K_TENOR), K_MATURITY));
	}

}
