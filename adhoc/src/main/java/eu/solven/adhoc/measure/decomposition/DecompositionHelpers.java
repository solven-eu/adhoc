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
package eu.solven.adhoc.measure.decomposition;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.MeasurelessQuery.MeasurelessQueryBuilder;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import lombok.experimental.UtilityClass;

/**
 * Helpers related to {@link IDecomposition}.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class DecompositionHelpers {

	private static final Predicate<IColumnFilter> ON_MISSING_IS_MATCHING = filterMissingColumn -> {
		// The group column is not filtered: accept this group as it is not rejected
		// e.g. we filter color=pink: it should not reject countryGroup=G8
		return true;
	};

	public static Predicate<IColumnFilter> onMissingColumn() {
		return ON_MISSING_IS_MATCHING;
	}

	/**
	 * 
	 * @param step
	 * @param column
	 * @return a {@link MeasurelessQuery} where given column has been suppressed (i.e. removed from
	 *         {@link IAdhocGroupBy} and filter are turned into `.matchAll`).
	 */
	public static MeasurelessQuery suppressColumn(IWhereGroupByQuery step, String column) {
		MeasurelessQueryBuilder underlyingStep = MeasurelessQuery.edit(step);

		if (step.getGroupBy().getGroupedByColumns().contains(column)) {
			// Underlying measure handles an array: `scenarioIndex` is meaningless
			Map<String, IAdhocColumn> groupByWithoutIndex = new LinkedHashMap<>(step.getGroupBy().getNameToColumn());
			groupByWithoutIndex.remove(column);
			underlyingStep.groupBy(GroupByColumns.of(groupByWithoutIndex.values())).build();
		}

		if (FilterHelpers.getFilteredColumns(step.getFilter()).contains(column)) {
			// Underlying measure handles an array: `scenarioIndex` is meaningless
			IAdhocFilter supressedFilter = SimpleFilterEditor.suppressColumn(step.getFilter(), Set.of(column));

			underlyingStep.filter(supressedFilter);
			// BEWARE In a different design, we should ensure we query only the relevant underlying double columns.
			// This is not done here as it would require more coupled logics
		}

		return underlyingStep.build();
	}

	public static MeasurelessQuery suppressColumn(IWhereGroupByQuery step, Set<String> columns) {
		MeasurelessQuery underlyingStep = MeasurelessQuery.edit(step).build();

		for (String column : columns) {
			underlyingStep = suppressColumn(underlyingStep, column);
		}

		return underlyingStep;
	}
}
