/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.measure.decomposition.many2many;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.decomposition.DecompositionHelpers;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.IDecompositionEntry;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterMatcher;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.IOrFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.extern.slf4j.Slf4j;

/**
 * This is an example {@link IDecomposition}, which copy the underlying value for each output associated to the input
 * value.
 * <p>
 * For instance, given v=200 on element=FR, we write v=200 into group=G8 and group=G20.
 * 
 * It is one-dimensional as it depends on a single input columns
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class ManyToMany1DDecomposition implements IDecomposition {
	public static final String KEY = "many_to_many_1d";
	/**
	 * The column used as elements: the underlying measure is expressed on this column
	 */
	public static final String K_INPUT = "element";
	/**
	 * The column written by this decomposition.
	 */
	public static final String K_OUTPUT = "group";

	final Map<String, ?> options;

	final IManyToMany1DDefinition manyToManyDefinition;

	public ManyToMany1DDecomposition(Map<String, ?> options) {
		this(options, new ManyToMany1DInMemoryDefinition());

		log.warn("Instantiated with default/empty {}", this.manyToManyDefinition);
	}

	public ManyToMany1DDecomposition(Map<String, ?> options, IManyToMany1DDefinition manyToManyDefinition) {
		this.options = options;
		this.manyToManyDefinition = manyToManyDefinition;

		String elementColumn = MapPathGet.getRequiredString(options, K_INPUT);
		String groupColumn = MapPathGet.getRequiredString(options, K_OUTPUT);

		if (elementColumn.equals(groupColumn)) {
			throw new UnsupportedOperationException("TODO This case requires specific behaviors and unitTests");
		}
	}

	@Override
	public List<IDecompositionEntry> decompose(ISliceWithStep slice, Object value) {
		String elementColumn = MapPathGet.getRequiredString(options, K_INPUT);

		Optional<?> optInput = slice.optSliced(elementColumn);
		if (optInput.isEmpty()) {
			// There is no expressed element
			return List.of(IDecompositionEntry.of(Map.of(), value));
		}

		Object element = optInput.get();

		if (element instanceof Collection<?>) {
			throw new UnsupportedOperationException("TODO Handle element being a Collection");
		}

		Set<Object> groups = getGroups(slice, element);

		String groupColumn = MapPathGet.getRequiredString(options, K_OUTPUT);

		return makeDecomposition(element,
				value,
				groupColumn,
				groups,
				slice.getQueryStep().getGroupBy().getGroupedByColumns().contains(groupColumn));
	}

	/**
	 * 
	 * @param element
	 *            the coordinate along the input column. e.g. `FR` if the input column were `country`.
	 * @param value
	 *            the value of the underlying measure for given element
	 * @param groupColumn
	 *            the name of the grouped column (e.g. `countryGroup`)
	 * @param groups
	 *            the groups in which given element are duplicated
	 * @param isGroupedByGroup
	 *            if true, the query is groupedBy the group column. Else, the received groups are present as filter but
	 *            not groupedBy.
	 * @return
	 */
	protected List<IDecompositionEntry> makeDecomposition(Object element,
			Object value,
			String groupColumn,
			Set<Object> groups,
			boolean isGroupedByGroup) {
		if (isGroupedByGroup) {
			// One contribution per filtered groups
			return groups.stream()
					.map(group -> IDecompositionEntry.of(Map.of(groupColumn, group), scale(element, value)))
					.toList();
		} else {
			// A single contribution for the filtered groups
			return List.of(IDecompositionEntry.of(Map.of(), scale(groups, value)));
		}
	}

	protected Set<Object> getGroups(ISliceWithStep slice, Object element) {
		Set<Object> groupsMayBeFilteredOut = manyToManyDefinition.getGroups(element);

		Set<String> queryMatchingGroups = getQueryMatchingGroups(slice);

		Set<Object> matchingGroups =
				ManyToManyNDDecomposition.<Object>intersection(queryMatchingGroups, groupsMayBeFilteredOut);

		log.debug("Element={} led to accepted groups={}", element, matchingGroups);

		return matchingGroups;
	}

	protected Set<String> getQueryMatchingGroups(ISliceWithStep slice) {
		Map<Object, Object> queryStepCache = slice.getQueryStep().getCache();

		// The groups valid given the filter: we compute it only once as an element may matches many groups: we do not
		// want to filter all groups for each element
		Object queryMatchingGroups = queryStepCache.computeIfAbsent("matchingGroups", cacheKey -> {
			return normalize(getQueryMatchingGroupsNoCache(slice));
		});
		return (Set<String>) queryMatchingGroups;
	}

	protected Set<?> normalize(Set<?> coordinates) {
		return AdhocPrimitiveHelpers.normalizeValues(coordinates);
	}

	protected Set<?> getQueryMatchingGroupsNoCache(ISliceWithStep slice) {
		String groupColumn = MapPathGet.getRequiredString(options, K_OUTPUT);

		IAdhocFilter filter = slice.getQueryStep().getFilter();

		return manyToManyDefinition.getMatchingGroups(group -> doFilterGroup(filter, groupColumn, group));
	}

	protected boolean doFilterGroup(IAdhocFilter filter, String groupColumn, Object groupCandidate) {
		return FilterMatcher.builder()
				.filter(filter)
				.onMissingColumn(DecompositionHelpers.onMissingColumn())
				.build()
				.match(Map.of(groupColumn, groupCandidate));
	}

	/**
	 * @param input
	 * @param value
	 * @return the value to attach to given group.
	 */
	protected Object scale(Object input, Object value) {
		// By default, we duplicate the value for each group
		return value;
	}

	@Override
	public List<IWhereGroupByQuery> getUnderlyingSteps(CubeQueryStep step) {
		String elementColumn = MapPathGet.getRequiredString(options, K_INPUT);
		String groupColumn = MapPathGet.getRequiredString(options, K_OUTPUT);

		IAdhocFilter requestedFilter = step.getFilter();
		IAdhocFilter underlyingFilter = convertGroupsToElementsFilter(groupColumn, elementColumn, requestedFilter);

		if (!step.getGroupBy().getGroupedByColumns().contains(groupColumn)) {
			// None of the requested column is an output column of this decomposition : there is nothing to decompose
			return Collections.singletonList(MeasurelessQuery.edit(step).filter(underlyingFilter).build());
		}

		// If we are requested on the dispatched level, we have to groupBy the input level
		Set<IAdhocColumn> allGroupBys = new HashSet<>();
		allGroupBys.addAll(step.getGroupBy().getNameToColumn().values());
		// The groupColumn is generally meaningless to the underlying measure
		allGroupBys.removeIf(c -> c.getName().equals(groupColumn));

		String inputColumn = MapPathGet.getRequiredString(options, K_INPUT);
		allGroupBys.add(ReferencedColumn.ref(inputColumn));

		// TODO If we filter some group, we should propagate as filtering some element
		// step.getFilter().

		return Collections.singletonList(
				MeasurelessQuery.edit(step).filter(underlyingFilter).groupBy(GroupByColumns.of(allGroupBys)).build());
	}

	protected IAdhocFilter convertGroupsToElementsFilter(String groupColumn,
			String elementColumn,
			IAdhocFilter requestedFilter) {
		IAdhocFilter underlyingFilter;

		if (requestedFilter.isMatchAll()) {
			underlyingFilter = requestedFilter;
		} else if (requestedFilter.isColumnFilter() && requestedFilter instanceof IColumnFilter columnFilter) {
			// If it is the group column which is filtered, convert it into an element filter
			if (columnFilter.getColumn().equals(groupColumn)) {
				// Plain filter on the group column: transform it into a filter into the input column
				Set<?> elements = elementsMatchingGroups(columnFilter.getValueMatcher());
				IAdhocFilter elementAdditionalFilter = ColumnFilter.isIn(elementColumn, elements);

				underlyingFilter = elementAdditionalFilter;
			} else {
				underlyingFilter = requestedFilter;
			}
		} else if (requestedFilter.isAnd() && requestedFilter instanceof IAndFilter andFilter) {
			List<IAdhocFilter> elementsFilters = andFilter.getOperands()
					.stream()
					.map(a -> convertGroupsToElementsFilter(groupColumn, elementColumn, a))
					.toList();

			underlyingFilter = AndFilter.and(elementsFilters);
		} else if (requestedFilter.isOr() && requestedFilter instanceof IOrFilter orFilter) {
			List<IAdhocFilter> elementsFilters = orFilter.getOperands()
					.stream()
					.map(a -> convertGroupsToElementsFilter(groupColumn, elementColumn, a))
					.toList();

			underlyingFilter = OrFilter.or(elementsFilters);
		} else {
			throw new UnsupportedOperationException("TODO handle requestedFilter=%s".formatted(requestedFilter));
		}

		return underlyingFilter;
	}

	protected Set<?> elementsMatchingGroups(IValueMatcher valueMatcher) {
		return manyToManyDefinition.getElementsMatchingGroups(valueMatcher);
	}

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		return ImmutableMap.<String, Class<?>>builder()
				.put(MapPathGet.getRequiredString(options, K_OUTPUT), Object.class)
				.build();
	}

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		Set<Object> groups = manyToManyDefinition.getGroups(valueMatcher);
		return CoordinatesSample.builder()
				.estimatedCardinality(CoordinatesSample.NO_ESTIMATION)
				.coordinates(groups)
				.build();
	}

}
