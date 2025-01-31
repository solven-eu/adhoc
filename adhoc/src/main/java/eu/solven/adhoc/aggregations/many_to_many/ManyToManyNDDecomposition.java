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
package eu.solven.adhoc.aggregations.many_to_many;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import eu.solven.adhoc.aggregations.IDecomposition;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.cube.IWhereGroupbyAdhocQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.IOrFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.query.groupby.IAdhocColumn;
import eu.solven.adhoc.query.groupby.ReferencedColumn;
import eu.solven.adhoc.slice.IAdhocSliceWithStep;
import eu.solven.pepper.mappath.MapPathGet;
import lombok.extern.slf4j.Slf4j;

/**
 * Similar to {@link ManyToMany1DDecomposition}, but relying on multiple input columns. It is useful when the output
 * group depends on multiple input groups.
 * 
 * For instance, if G8 and G20 were changing through time, these country groups would be defined given input columns
 * (country, date).
 * 
 * @see ManyToMany1DDecomposition
 */
@Slf4j
public class ManyToManyNDDecomposition implements IDecomposition {
	public static final String KEY = "many_to_many";
	/**
	 * The columns used as elements: the underlying measure is expressed on these columns
	 */
	public static final String K_INPUTS = "elements";
	/**
	 * The column written by this decomposition.
	 */
	public static final String K_OUTPUT = "group";

	final Map<String, ?> options;

	final IManyToManyNDDefinition manyToManyDefinition;

	public ManyToManyNDDecomposition(Map<String, ?> options) {
		this(options, new ManyToManyNDInMemoryDefinition());

		log.warn("Instantiated with default/empty {}", this.manyToManyDefinition);
	}

	public ManyToManyNDDecomposition(Map<String, ?> options, IManyToManyNDDefinition manyToManyDefinition) {
		this.options = options;
		this.manyToManyDefinition = manyToManyDefinition;

		Set<String> elementColumns = getInputColumns(options);
		String groupColumn = MapPathGet.getRequiredString(options, K_OUTPUT);

		if (elementColumns.contains(groupColumn)) {
			throw new UnsupportedOperationException("TODO This case requires specific behaviors and unitTests");
		}
	}

	protected Set<String> getInputColumns(Map<String, ?> options) {
		Collection<String> rawInputColumns = MapPathGet.getRequiredAs(options, K_INPUTS);
		return ImmutableSet.copyOf(rawInputColumns);
	}

	protected Set<Object> getQueryMatchingGroups(IAdhocSliceWithStep slice) {
		Map<Object, Object> queryStepCache = slice.getQueryStep().getCache();

		// The groups valid given the filter: we compute it only once as an element may matches many groups: we do not
		// want to filter all groups for each element
		Set<Object> queryMatchingGroups = (Set<Object>) queryStepCache.computeIfAbsent("matchingGroups", cacheKey -> {
			return getQueryStepMatchingGroupsNoCache(slice);
		});
		return queryMatchingGroups;
	}

	protected Set<?> getQueryStepMatchingGroupsNoCache(IAdhocSliceWithStep slice) {
		String groupColumn = MapPathGet.getRequiredString(options, K_OUTPUT);

		IAdhocFilter filter = slice.getQueryStep().getFilter();

		return manyToManyDefinition.getMatchingGroups(group -> doFilterGroup(groupColumn, group, filter));
	}

	@Override
	public Map<Map<String, ?>, Object> decompose(IAdhocSliceWithStep slice, Object value) {
		Set<String> elementColumns = getInputColumns(options);

		Map<String, ?> elementCoordinates = slice.optFilters(elementColumns);
		if (elementCoordinates.size() < elementColumns.size()) {
			// We lack some coordinates
			return Map.of(Map.of(), value);
		}

		Set<Object> groups = getGroups(slice, elementCoordinates);

		String groupColumn = MapPathGet.getRequiredString(options, K_OUTPUT);

		return makeDecomposition(elementCoordinates, value, groupColumn, groups);
	}

	protected Map<Map<String, ?>, Object> makeDecomposition(Map<String, ?> element,
			Object value,
			String groupColumn,
			Set<Object> groups) {
		Map<Map<String, ?>, Object> output = new HashMap<>();

		groups.forEach(group -> {
			output.put(Map.of(groupColumn, group), scale(element, value));
		});

		return output;
	}

	protected Set<Object> getGroups(IAdhocSliceWithStep slice, Map<String, ?> columnToElement) {
		Set<Object> queryMatchingGroups = getQueryMatchingGroups(slice);

		Set<Object> groupsMayBeFilteredOut = manyToManyDefinition
				.getGroups(InMatcher.builder().operands(queryMatchingGroups).build(), columnToElement);

		Set<Object> matchingGroups = intersection(queryMatchingGroups, groupsMayBeFilteredOut);

		log.debug("Element={} led to accepted groups={}", columnToElement, matchingGroups);

		return matchingGroups;
	}

	public static <E extends Object> SetView<E> intersection(final Set<? extends E> set1, final Set<? extends E> set2) {
		SetView<E> intersection;

		// Sets.intersection will iterate over the first Set: we observe it is faster the consider the smaller Set first
		if (set1.size() < set2.size()) {
			intersection = Sets.intersection(Collections.unmodifiableSet(set1), set2);
		} else {
			intersection = Sets.intersection(Collections.unmodifiableSet(set2), set1);
		}
		return intersection;
	}

	protected boolean doFilterGroup(String groupColumn, Object groupCandidate, IAdhocFilter filter) {
		if (filter.isMatchAll()) {
			return true;
		} else if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			if (!columnFilter.getColumn().equals(groupColumn)) {
				// The group column is not filtered: accept this group as it is not rejected
				// e.g. we filter color=pink: it should not reject countryGroup=G8
				return true;
			} else {
				boolean match = columnFilter.getValueMatcher().match(groupCandidate);

				log.debug("{} is matched", groupCandidate);

				return match;
			}
		} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			return andFilter.getOperands()
					.stream()
					.allMatch(subFilter -> doFilterGroup(groupColumn, groupCandidate, subFilter));
		} else if (filter.isOr() && filter instanceof IOrFilter orFilter) {
			return orFilter.getOperands()
					.stream()
					.anyMatch(subFilter -> doFilterGroup(groupColumn, groupCandidate, subFilter));
		} else {
			throw new UnsupportedOperationException("%s is not managed".formatted(filter));
		}
	}

	/**
	 * @param input
	 * @param value
	 * @return the value to attach to given group.
	 */
	protected Object scale(Map<String, ?> input, Object value) {
		// By default, we duplicate the value for each group
		return value;
	}

	@Override
	public List<IWhereGroupbyAdhocQuery> getUnderlyingSteps(AdhocQueryStep step) {
		Set<String> elementColumns = getInputColumns(options);
		String groupColumn = MapPathGet.getRequiredString(options, K_OUTPUT);

		IAdhocFilter requestedFilter = step.getFilter();
		IAdhocFilter underlyingFilter = convertGroupsToElementsFilter(groupColumn, requestedFilter);

		if (!step.getGroupBy().getGroupedByColumns().contains(groupColumn)) {
			// None of the requested column is an output column of this decomposition : there is nothing to decompose
			return Collections.singletonList(MeasurelessQuery.edit(step).filter(underlyingFilter).build());
		}

		// If we are requested on the dispatched level, we have to groupBy the input level
		Set<IAdhocColumn> allGroupBys = new HashSet<>();
		allGroupBys.addAll(step.getGroupBy().getNameToColumn().values());
		// The groupColumn is generally meaningless to the underlying measure
		allGroupBys.removeIf(c -> c.getColumn().equals(groupColumn));

		elementColumns.forEach(elementColumn -> allGroupBys.add(ReferencedColumn.ref(elementColumn)));

		// TODO If we filter some group, we should propagate as filtering some element
		// step.getFilter().

		return Collections.singletonList(
				MeasurelessQuery.edit(step).filter(underlyingFilter).groupBy(GroupByColumns.of(allGroupBys)).build());
	}

	protected IAdhocFilter convertGroupsToElementsFilter(String groupColumn, IAdhocFilter requestedFilter) {
		IAdhocFilter underlyingFilter;

		if (requestedFilter.isMatchAll()) {
			underlyingFilter = requestedFilter;
		} else if (requestedFilter.isColumnFilter() && requestedFilter instanceof IColumnFilter columnFilter) {
			// If it is the group column which is filtered, convert it into an element filter
			if (columnFilter.getColumn().equals(groupColumn)) {
				// Plain filter on the group column: transform it into a filter into the input column
				Set<Map<String, IValueMatcher>> elements = elementsMatchingGroups(columnFilter.getValueMatcher());

				Set<IAdhocFilter> elementsFilters = elements.stream()
						.map(groupElements -> AndFilter.and(groupElements))
						.collect(Collectors.toSet());

				IAdhocFilter elementAdditionalFilter = OrFilter.or(elementsFilters);

				underlyingFilter = elementAdditionalFilter;
			} else {
				underlyingFilter = requestedFilter;
			}
		} else if (requestedFilter.isAnd() && requestedFilter instanceof IAndFilter andFilter) {
			List<IAdhocFilter> elementsFilters =
					andFilter.getOperands().stream().map(a -> convertGroupsToElementsFilter(groupColumn, a)).toList();

			underlyingFilter = AndFilter.and(elementsFilters);
		} else if (requestedFilter.isOr() && requestedFilter instanceof IOrFilter orFilter) {
			List<IAdhocFilter> elementsFilters =
					orFilter.getOperands().stream().map(a -> convertGroupsToElementsFilter(groupColumn, a)).toList();

			underlyingFilter = OrFilter.or(elementsFilters);
		} else {
			throw new UnsupportedOperationException("TODO handle requestedFilter=%s".formatted(requestedFilter));
		}

		return underlyingFilter;
	}

	protected Set<Map<String, IValueMatcher>> elementsMatchingGroups(IValueMatcher valueMatcher) {
		return manyToManyDefinition.getElementsMatchingGroups(valueMatcher);
	}

}
