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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.decomposition.DecompositionHelpers;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.IDecompositionEntry;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.FilterMatcher;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.IOrFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.util.NotYetImplementedException;
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
 * 
 * @author Benoit Lacelle
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

	@SuppressWarnings("PMD.ConstructorCallsOverridableMethod")
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

	protected Set<Object> getQueryMatchingGroups(ISliceWithStep slice) {
		Map<Object, Object> queryStepCache = slice.getQueryStep().getCache();

		// The groups valid given the filter: we compute it only once as an element may matches many groups: we do not
		// want to filter all groups for each element
		return (Set<Object>) queryStepCache.computeIfAbsent("matchingGroups", cacheKey -> {
			return getQueryStepMatchingGroupsNoCache(slice);
		});
	}

	protected Set<?> getQueryStepMatchingGroupsNoCache(ISliceWithStep slice) {
		String groupColumn = MapPathGet.getRequiredString(options, K_OUTPUT);

		ISliceFilter filter = slice.getQueryStep().getFilter();

		return manyToManyDefinition.getMatchingGroups(group -> doFilterGroup(filter, groupColumn, group));
	}

	@Override
	public List<IDecompositionEntry> decompose(ISliceWithStep slice, Object value) {
		Set<String> elementColumns = getInputColumns(options);

		Map<String, ?> elementCoordinates = slice.getSlice().optSliced(elementColumns);
		if (elementCoordinates.size() < elementColumns.size()) {
			// We lack some coordinates
			return List.of(IDecompositionEntry.of(Map.of(), value));
		}

		Set<Object> groups = getGroups(slice, elementCoordinates);

		String groupColumn = MapPathGet.getRequiredString(options, K_OUTPUT);

		return makeDecomposition(elementCoordinates, value, groupColumn, groups);
	}

	protected List<IDecompositionEntry> makeDecomposition(Map<String, ?> element,
			Object value,
			String groupColumn,
			Set<Object> groups) {
		List<IDecompositionEntry> output = new ArrayList<>(groups.size());

		groups.forEach(group -> {
			output.add(IDecompositionEntry.of(Map.of(groupColumn, group), scale(element, value)));
		});

		return output;
	}

	protected Set<Object> getGroups(ISliceWithStep slice, Map<String, ?> columnToElement) {
		Set<Object> queryMatchingGroups = getQueryMatchingGroups(slice);

		Set<Object> groupsMayBeFilteredOut = manyToManyDefinition
				.getGroups(InMatcher.builder().operands(queryMatchingGroups).build(), columnToElement);

		Set<Object> matchingGroups = intersection(queryMatchingGroups, groupsMayBeFilteredOut);

		log.debug("Element={} led to accepted groups={}", columnToElement, matchingGroups);

		return matchingGroups;
	}

	@SuppressWarnings("PMD.LooseCoupling")
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

	protected boolean doFilterGroup(ISliceFilter filter, String groupColumn, Object groupCandidate) {
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
	protected Object scale(Map<String, ?> input, Object value) {
		// By default, we duplicate the value for each group
		return value;
	}

	@Override
	public List<IWhereGroupByQuery> getUnderlyingSteps(CubeQueryStep step) {
		Set<String> elementColumns = getInputColumns(options);
		String groupColumn = MapPathGet.getRequiredString(options, K_OUTPUT);

		ISliceFilter requestedFilter = step.getFilter();
		ISliceFilter underlyingFilter = convertGroupsToElementsFilter(groupColumn, requestedFilter);

		if (!step.getGroupBy().getGroupedByColumns().contains(groupColumn)) {
			// None of the requested column is an output column of this decomposition : there is nothing to decompose
			return Collections.singletonList(MeasurelessQuery.edit(step).filter(underlyingFilter).build());
		}

		// If we are requested on the dispatched level, we have to groupBy the input level
		Set<IAdhocColumn> allGroupBys = new HashSet<>();
		allGroupBys.addAll(step.getGroupBy().getNameToColumn().values());
		// The groupColumn is generally meaningless to the underlying measure
		allGroupBys.removeIf(c -> c.getName().equals(groupColumn));

		elementColumns.forEach(elementColumn -> allGroupBys.add(ReferencedColumn.ref(elementColumn)));

		// TODO If we filter some group, we should propagate as filtering some element
		// step.getFilter().

		return Collections.singletonList(
				MeasurelessQuery.edit(step).filter(underlyingFilter).groupBy(GroupByColumns.of(allGroupBys)).build());
	}

	protected ISliceFilter convertGroupsToElementsFilter(String groupColumn, ISliceFilter requestedFilter) {
		ISliceFilter underlyingFilter;

		if (requestedFilter.isMatchAll()) {
			underlyingFilter = requestedFilter;
		} else if (requestedFilter.isColumnFilter() && requestedFilter instanceof IColumnFilter columnFilter) {
			// If it is the group column which is filtered, convert it into an element filter
			if (columnFilter.getColumn().equals(groupColumn)) {
				// Plain filter on the group column: transform it into a filter into the input column
				Set<Map<String, IValueMatcher>> elements = elementsMatchingGroups(columnFilter.getValueMatcher());

				Set<ISliceFilter> elementsFilters = elements.stream().map(AndFilter::and).collect(Collectors.toSet());

				ISliceFilter elementAdditionalFilter = FilterBuilder.or(elementsFilters).optimize();

				underlyingFilter = elementAdditionalFilter;
			} else {
				underlyingFilter = requestedFilter;
			}
		} else if (requestedFilter.isAnd() && requestedFilter instanceof IAndFilter andFilter) {
			List<ISliceFilter> elementsFilters =
					andFilter.getOperands().stream().map(a -> convertGroupsToElementsFilter(groupColumn, a)).toList();

			underlyingFilter = FilterBuilder.and(elementsFilters).optimize();
		} else if (requestedFilter.isOr() && requestedFilter instanceof IOrFilter orFilter) {
			List<ISliceFilter> elementsFilters =
					orFilter.getOperands().stream().map(a -> convertGroupsToElementsFilter(groupColumn, a)).toList();

			underlyingFilter = FilterBuilder.or(elementsFilters).optimize();
		} else {
			throw new NotYetImplementedException("TODO handle requestedFilter=%s".formatted(requestedFilter));
		}

		return underlyingFilter;
	}

	protected Set<Map<String, IValueMatcher>> elementsMatchingGroups(IValueMatcher valueMatcher) {
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
		Set<?> groups = manyToManyDefinition.getMatchingGroups(valueMatcher);
		return CoordinatesSample.builder()
				.estimatedCardinality(CoordinatesSample.NO_ESTIMATION)
				.coordinates(groups)
				.build();
	}

}
