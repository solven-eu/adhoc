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
import java.util.NavigableSet;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

/**
 * Demonstrate how to duplicate some value along some generated columns.
 * 
 * Its usage is generally sub-optimal, but it enables simpler designs.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class DuplicatingDecomposition implements IDecomposition {
	@Singular
	@NonNull
	Map<String, Collection<?>> columnToCoordinates;

	final Supplier<Map<String, Class<?>>> columnToTypeSupplier = Suppliers.memoize(() -> {
		Map<String, Class<?>> columnToType = new LinkedHashMap<>();

		this.columnToCoordinates.forEach((column, coordinates) -> {
			columnToType.put(column, getColumnClass(column, coordinates));
		});

		return columnToType;
	});

	public DuplicatingDecomposition(Map<String, ?> options) {
		columnToCoordinates = MapPathGet.getRequiredMap(options, "columnToCoordinates");
	}

	private Class<?> getColumnClass(String column, Collection<?> coordinates) {
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
		Collection<?> coordinates = columnToCoordinates.get(column);
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
		NavigableSet<String> groupedByColumns = slice.getQueryStep().getGroupBy().getGroupedByColumns();

		// Do a copy as we will iterate often over this List
		List<String> relevantColumns =
				ImmutableList.copyOf(Sets.intersection(groupedByColumns, columnToCoordinates.keySet()));

		List<List<?>> indexToCoordinates = new ArrayList<>(relevantColumns.size());
		for (String relevantColumn : relevantColumns) {
			indexToCoordinates.add(ImmutableList.copyOf(columnToCoordinates.get(relevantColumn)));
		}

		List<IDecompositionEntry> decompositions = new ArrayList<>();

		// TODO Design-Issue: we should be able to return an Iterable not to materialize too early such a
		// cartesianProduct
		Lists.cartesianProduct(indexToCoordinates).forEach(coordinates -> {
			Map<String, Object> projectedSlice = new LinkedHashMap<>();

			for (int i = 0; i < relevantColumns.size(); i++) {
				projectedSlice.put(relevantColumns.get(i), coordinates.get(i));
			}

			decompositions.add(IDecompositionEntry.of(projectedSlice, value));
		});

		log.debug("CartesianProduct led to {} decompositions for slice={} and value={}",
				decompositions.size(), slice, value);

		return decompositions;
	}

	@Override
	public List<IWhereGroupByQuery> getUnderlyingSteps(CubeQueryStep step) {
		// Suppress duplicated columns as they are supposedly not provided by the ITableWrapper
		return List.of(IDecomposition.suppressColumn(step, columnToCoordinates.keySet()));
	}

}
