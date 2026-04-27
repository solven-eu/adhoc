/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure.decomposition.join;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.engine.step.IWhereGroupByQuery;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.editor.SimpleFilterEditor;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.IDecompositionEntry;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.util.NotYetImplementedException;

/**
 * An {@link IDecomposition} that performs a lookup-JOIN producing <em>multiple</em> output columns at once. Given a set
 * of input columns (the join keys), it resolves a full row of output columns from an {@link IJoinDefinition} —
 * conceptually a SQL {@code LEFT JOIN} on a small enrichment/dimension table.
 *
 * <p>
 * Example: input columns {@code (asOfDate, country)} → output columns {@code (weather, language)}. The lookup table
 * maps {@code {asOfDate=2026-04-17, country=FR}} to {@code {weather=sunny, language=French}}.
 *
 * <p>
 * When any output column appears in the query's GROUP BY or filter, the decomposition automatically adds the input
 * columns to the underlying step's GROUP BY and strips filters on output columns (they are applied later by
 * {@code DispatchorQueryStep}).
 *
 * @author Benoit Lacelle
 */
public class JoinDecomposition implements IDecomposition {
	public static final String KEY = "join";
	public static final String K_OUTPUTS = "outputs";
	public static final String K_INPUTS = "inputs";

	final ImmutableSet<String> inputColumns;
	final ImmutableSet<String> outputColumns;
	final IJoinDefinition joinDefinition;

	public JoinDecomposition(Map<String, ?> options, IJoinDefinition joinDefinition) {
		this.outputColumns = toStringList(options.get(K_OUTPUTS), K_OUTPUTS);
		this.inputColumns = toStringList(options.get(K_INPUTS), K_INPUTS);
		this.joinDefinition = joinDefinition;

		if (!Sets.intersection(outputColumns, inputColumns).isEmpty()) {
			// TODO Could this be lifted by managing as some sort of renaming
			throw new NotYetImplementedException("Can not have a column being both an input and an output");
		}
	}

	@SuppressWarnings("unchecked")
	private static ImmutableSet<String> toStringList(Object raw, String optionName) {
		if (raw instanceof Collection<?> coll) {
			return coll.stream().map(Object::toString).collect(ImmutableSet.toImmutableSet());
		}
		throw new IllegalArgumentException("Option '%s' must be a Collection of column names".formatted(optionName));
	}

	@Override
	public List<IDecompositionEntry> decompose(ISliceWithStep slice, Object value) {
		// Build the composite key from the slice's coordinates on the input columns.
		Map<String, Object> joinKey = new LinkedHashMap<>();
		for (String inputCol : inputColumns) {
			Optional<?> coord = slice.sliceReader().extractCoordinateLax(inputCol, Object.class);
			if (coord.isEmpty()) {
				return ImmutableList.of(IDecompositionEntry.of(ImmutableMap.of(), value));
			}
			joinKey.put(inputCol, coord.get());
		}

		Optional<Map<String, Object>> outputRow = joinDefinition.lookup(joinKey);
		if (outputRow.isEmpty()) {
			return ImmutableList.of(IDecompositionEntry.of(ImmutableMap.of(), value));
		}

		return ImmutableList.of(IDecompositionEntry.of(outputRow.get(), value));
	}

	@Override
	public List<IWhereGroupByQuery> getUnderlyingSteps(CubeQueryStep step) {
		Set<String> groupByColumns = step.getGroupBy().getSortedColumns();
		Set<String> filteredColumns = FilterHelpers.getFilteredColumns(step.getFilter());

		boolean anyOutputInGroupBy = !Sets.intersection(outputColumns, groupByColumns).isEmpty();
		boolean anyOutputInFilter = !Sets.intersection(outputColumns, filteredColumns).isEmpty();

		// Strip filters on ALL output columns.
		ISliceFilter underlyingFilter = SimpleFilterEditor.suppressColumn(step.getFilter(), outputColumns);

		if (!anyOutputInGroupBy && !anyOutputInFilter) {
			return ImmutableList.of(MeasurelessQuery.edit(step).filter(underlyingFilter).build());
		}

		// Add input columns so the JOIN can be evaluated
		Set<IAdhocColumn> groupBys = new LinkedHashSet<>(step.getGroupBy().getColumns());
		// remove output columns (they're generated).
		groupBys.removeIf(c -> outputColumns.contains(c.getName()));

		for (String inputCol : inputColumns) {
			groupBys.add(ReferencedColumn.ref(inputCol));
		}

		return ImmutableList
				.of(MeasurelessQuery.edit(step).filter(underlyingFilter).groupBy(GroupByColumns.of(groupBys)).build());
	}

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		return joinDefinition.getColumnTypes();
	}

	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		if (!outputColumns.contains(column)) {
			return CoordinatesSample.empty();
		}
		Set<Object> allValues = joinDefinition.allOutputValues(column);
		return CoordinatesSample.builder()
				.coordinates(allValues.stream()
						.limit(limit < 0 ? Long.MAX_VALUE : limit)
						.collect(ImmutableList.toImmutableList()))
				.estimatedCardinality(allValues.size())
				.build();
	}
}
