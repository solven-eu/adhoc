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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.FilterMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.NonNull;
import lombok.Singular;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Demonstrate how to duplicate some value along some generated columns.
 * 
 * Its usage is generally sub-optimal, but it enables simpler designs.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
public class DuplicatingDecomposition implements IDecomposition {
	@Singular
	@NonNull
	Map<String, Collection<?>> columnToCoordinates;

	final Supplier<Map<String, Class<?>>> columnToTypeSupplier = Suppliers.memoize(() -> {
		Map<String, Class<?>> columnToType = new LinkedHashMap<>();

		getDuplicatedColumns().forEach(column -> {
			Class<?> columnClass = getColumnClass(column, getDefaultCoordinates(column));
			columnToType.put(column, columnClass);
		});

		return columnToType;
	});

	public DuplicatingDecomposition(Map<String, ?> options) {
		columnToCoordinates = MapPathGet.getRequiredMap(options, "columnToCoordinates");
	}

	protected Set<String> getDuplicatedColumns() {
		return columnToCoordinates.keySet();
	}

	protected Collection<?> getDefaultCoordinates(String column) {
		return columnToCoordinates.get(column);
	}

	/**
	 * 
	 * @param relevantColumn
	 * @param value
	 *            the aggregate may help computing the coordinates along which the duplication should occur.
	 * @return the coordinates along which given value has to be duplicated.
	 */
	protected ImmutableList<?> getCoordinatesAlongColumn(String relevantColumn, Object value) {
		return ImmutableList.copyOf(columnToCoordinates.get(relevantColumn));
	}

	protected Class<?> getColumnClass(String column, Collection<?> coordinates) {
		if (coordinates.isEmpty()) {
			log.warn("No coordinates along {}. It could lead to unexpected empty views", column);
			return Object.class;
		} else {
			// BEWARE Would be safer to scan the whole List
			return coordinates.iterator().next().getClass();
		}
	}

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		Collection<?> coordinates = getDefaultCoordinates(column);
		if (coordinates == null) {
			throw new IllegalArgumentException("Unexpected column: " + column);
		}

		return CoordinatesSample.builder().estimatedCardinality(coordinates.size()).coordinates(coordinates).build();
	}

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		return columnToTypeSupplier.get();
	}

	@Override
	public List<IDecompositionEntry> decompose(ISliceWithStep slice, Object value) {
		// groupedBy are required to properly feed the requested slice
		// filtered are required to let DispatcherQueryStep properly filter the relevant slices
		Set<String> groupedByColumns = slice.getQueryStep().getGroupBy().getGroupedByColumns();

		// Do a copy as we will iterate often over this List
		List<String> duplicatedGroupedByColumns =
				ImmutableList.copyOf(Sets.intersection(groupedByColumns, getDuplicatedColumns()));

		List<List<?>> indexToGroupedByCoordinates = indexToCoordinates(slice, value, duplicatedGroupedByColumns);

		Set<String> filteredNotGroupedByColumns =
				Sets.difference(FilterHelpers.getFilteredColumns(slice.getQueryStep().getFilter()), groupedByColumns);

		// Do a copy as we will iterate often over this List
		List<String> duplicatedFilteredNotGroupedByColumns =
				ImmutableList.copyOf(Sets.intersection(filteredNotGroupedByColumns, getDuplicatedColumns()));

		List<List<?>> indexToFilteredNotGroupedByCoordinates =
				indexToCoordinates(slice, value, duplicatedFilteredNotGroupedByColumns);

		List<IDecompositionEntry> decompositions = new ArrayList<>();

		// TODO Design-Issue: we should be able to return an Iterable not to materialize too early such a
		// cartesianProduct
		Lists.cartesianProduct(indexToGroupedByCoordinates).forEach(coordinates -> {
			Map<String, Object> projectedSlice = new LinkedHashMap<>();

			for (int i = 0; i < duplicatedGroupedByColumns.size(); i++) {
				projectedSlice.put(duplicatedGroupedByColumns.get(i), coordinates.get(i));
			}

			boolean matchFilter = Lists.cartesianProduct(indexToFilteredNotGroupedByCoordinates)
					.stream()
					.anyMatch(filteredNotGroupedByCoordinates -> {
						Map<String, Object> fullSlice = new LinkedHashMap<>();

						fullSlice.putAll(slice.getAdhocSliceAsMap().getCoordinates());
						fullSlice.putAll(projectedSlice);

						// Relates with DispatchorQueryStep.isRelevant
						// This filtering is done in the decomposition, else we would have to send a IDecompositionEntry
						// for
						// each potential filter, which leads to more issues about de-duplicating the duplicates. (e.g.
						// on
						// `group=G8|G20`, we would send 2 decompositionEntry (one for G8 and one for G20), while only
						// one of
						// them should be kept).
						boolean isRelevant = FilterMatcher.builder()
								.filter(slice.getQueryStep().getFilter())
								.onMissingColumn(DecompositionHelpers.onMissingColumn())
								.build()
								.match(fullSlice);

						return isRelevant;
					});

			if (matchFilter) {
				// Add a decompositionEntry per requested groupBy
				// BEWARE We must not send multiple duplicated due to interactions with filteredNotGroupBy column
				decompositions.add(IDecompositionEntry.of(projectedSlice, value));
			} else {
				log.debug("Not a single slice matches the filter");
			}

		});

		log.debug("CartesianProduct led to {} decompositions for slice={} and value={}",
				decompositions.size(),
				slice,
				value);

		return decompositions;
	}

	private List<List<?>> indexToCoordinates(ISliceWithStep slice,
			Object value,
			List<String> duplicatedGroupedByColumns) {
		List<List<?>> indexToGroupedByCoordinates = new ArrayList<>(duplicatedGroupedByColumns.size());
		for (String relevantColumn : duplicatedGroupedByColumns) {
			ImmutableList<?> unfilteredCoordinates = getCoordinatesAlongColumn(relevantColumn, value);

			// Filter coordinates according to filters
			// BEWARE This is a 1D filtering: some combinations may be filtered (e.g. with an OR).
			IValueMatcher valueMatcher = FilterHelpers.getValueMatcherLax(slice.asFilter(), relevantColumn);
			indexToGroupedByCoordinates.add(unfilteredCoordinates.stream()
					.filter(valueMatcher::match)
					.collect(ImmutableList.toImmutableList()));
		}
		return indexToGroupedByCoordinates;
	}

	@Override
	public List<IWhereGroupByQuery> getUnderlyingSteps(CubeQueryStep step) {
		// Suppress duplicated columns as they are supposedly not provided by the ITableWrapper
		return List.of(DecompositionHelpers.suppressColumn(step, getDuplicatedColumns()));
	}

}
