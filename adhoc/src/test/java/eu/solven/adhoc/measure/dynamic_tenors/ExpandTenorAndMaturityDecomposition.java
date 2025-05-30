/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.measure.dynamic_tenors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.IDecompositionEntry;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.value.IValueMatcher;

/**
 * Expand {@link MarketRiskSensitivity} along all tenor x maturities.
 * 
 * @author Benoit Lacelle
 */
public class ExpandTenorAndMaturityDecomposition implements IDecomposition, IExamplePnLExplainConstant {

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		return switch (column) {
		case K_TENOR:
		case K_MATURITY: {
			yield CoordinatesSample.builder().estimatedCardinality(TENORS.size()).coordinates(TENORS).build();
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
		if (!(value instanceof MarketRiskSensitivity sensitivives)) {
			return List.of(IDecompositionEntry.of(Map.of(), value));
		}

		IValueMatcher tenorMatcher = FilterHelpers.getValueMatcher(slice.asFilter(), K_TENOR);
		IValueMatcher maturityMatcher = FilterHelpers.getValueMatcher(slice.asFilter(), K_MATURITY);

		List<IDecompositionEntry> decompositions = new ArrayList<>();

		NavigableSet<String> groupedByColumns = slice.getQueryStep().getGroupBy().getGroupedByColumns();

		sensitivives.getCoordinatesToDelta().object2DoubleEntrySet().forEach(e -> {
			Map<String, ?> sensitivityCoordinates = e.getKey();
			double doubleValue = e.getDoubleValue();

			Object tenor = sensitivityCoordinates.get(K_TENOR);
			Object maturity = sensitivityCoordinates.get(K_MATURITY);
			if (!tenorMatcher.match(tenor)) {
				return;
			} else if (!maturityMatcher.match(maturity)) {
				return;
			}

			boolean isTenorGroupedBy = groupedByColumns.contains(K_TENOR);
			boolean isMaturityGroupedBy = groupedByColumns.contains(K_MATURITY);

			if (isTenorGroupedBy) {
				if (isMaturityGroupedBy) {
					// grouped by tenor and maturity
					decompositions
							.add(IDecompositionEntry.of(Map.of(K_TENOR, tenor, K_MATURITY, maturity), doubleValue));
				} else {
					// grouped by tenor
					decompositions.add(IDecompositionEntry.of(Map.of(K_TENOR, tenor), doubleValue));
				}
			} else if (isMaturityGroupedBy) {
				// grouped by maturity
				decompositions.add(IDecompositionEntry.of(Map.of(K_MATURITY, maturity), doubleValue));
			} else {
				// none are grouped by
				decompositions.add(IDecompositionEntry.of(Map.of(), doubleValue));
			}
		});

		return decompositions;
	}

	@Override
	public List<IWhereGroupByQuery> getUnderlyingSteps(CubeQueryStep step) {
		// Suppress tenor and maturity as they are columns embedded in the underlying aggregate
		return List.of(IDecomposition.suppressColumn(step, Set.of(K_TENOR, K_MATURITY)));
	}

}
