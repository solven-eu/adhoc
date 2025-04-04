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
package eu.solven.adhoc.query.many_to_many;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.math.LongMath;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.many2many.IManyToMany1DDefinition;
import eu.solven.adhoc.measure.decomposition.many2many.ManyToMany1DDecomposition;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import lombok.NonNull;

/**
 * These unitTests are dedicated to check ManyToMany performances in case of large problems. We can encounter millions
 * of elements/groups and groups of millions of elements in real-life.
 * <p>
 * Here, we consider groups with exponential growth: n-th group contains 2^n elements. It is useful to check performance
 * when we have many not very small groups
 */
public class TestManyToManyAdhocQuery_Large_Exponential extends ADagTest implements IAdhocTestConstants {
	// This could be adjusted so that tests takes a few seconds to execute. Can be increased a lot to test bigger cases
	int maxGroupCardinality = 18;
	int maxElementCardinality = groupToMaxElement(maxGroupCardinality);

	// +1 to also write a value which is larger than the largest element of the largest group
	int maxElementInserted = maxElementCardinality + 2;

	private int groupToMaxElement(int group) {
		if (group > 29) {
			throw new IllegalArgumentException(
					"Given exponential growth, it is invalid to consider group=%s".formatted(group));
		}

		return (1 << group) - 1;
	}

	private static int elementToSmallestGroup(int element) {
		return 32 - Integer.numberOfLeadingZeros(element);
	}

	int smallElement = 5;
	int largeElement = maxElementCardinality - smallElement;

	int smallGroup = elementToSmallestGroup(smallElement);
	int largeGroup = elementToSmallestGroup(largeElement);

	IManyToManyGroupToElements groupToElements = new IManyToManyGroupToElements() {
		private IntStream streamMatchingGroups(IValueMatcher groupMatcher) {
			return IntStream.rangeClosed(0, maxGroupCardinality).filter(groupMatcher::match);
		}

		@Override
		public Set<?> getElementsMatchingGroups(IValueMatcher groupMatcher) {
			if (groupMatcher instanceof EqualsMatcher equalsMatcher
					&& equalsMatcher.getOperand() instanceof Integer group) {
				return groupToElements(group);
			} else {
				return streamMatchingGroups(groupMatcher).flatMap(group -> {
					return groupToElements(group).stream().mapToInt(i -> i);
				}).boxed().collect(Collectors.toSet());
			}
		}

		@Override
		public Set<?> getMatchingGroups(IValueMatcher groupMatcher) {
			return streamMatchingGroups(groupMatcher).boxed().collect(Collectors.toSet());
		}
	};

	// group=0 -> element=0
	// group=1 -> element=0|1
	// group=2 -> element=0|1|2|3
	// group=3 -> element=0|1|2|3|4|5|6|7
	// ...
	// group=G -> element=0|N|...|2^N-1
	// which implies
	// element=0 -> group=0|G
	// element=1 -> group=1|G
	// element=2 -> group=2|G
	// element=3 -> group=2|G
	// element=4 -> group=3|G
	// element=5 -> group=3|G
	// ...
	IManyToMany1DDefinition manyToManyDefinition =
			ManyToMany1DDynamicDefinition.builder().elementToGroups(rawElement -> {
				if (rawElement instanceof Integer element) {
					if (element < 0) {
						return Set.of();
					} else if (element > maxElementCardinality) {
						return Set.of();
					}
					return Collections.unmodifiableSet(
							ContiguousSet.create(Range.closed(elementToSmallestGroup(element), maxElementCardinality),
									DiscreteDomain.integers()));
				} else {
					return Set.of();
				}
			}).groupToElements(groupToElements).build();

	private Set<Integer> groupToElements(int group) {
		if (group < 0) {
			// Beware, it means we do not list all negative integers as elements of -1
			return Set.of();
		} else if (group > maxGroupCardinality) {
			// Beware, it means we do not list all integers larger than maxCardinality as elements of maxCardinality
			return Set.of();
		} else {
			return ContiguousSet.create(Range.closed(0, groupToMaxElement(group)), DiscreteDomain.integers());
		}
	}

	public final AdhocQueryEngine aqe =
			editEngine().operatorsFactory(makeOperatorsFactory(manyToManyDefinition)).build();
	public final CubeWrapper aqw = CubeWrapper.builder().table(table).engine(aqe).forest(forest).build();

	private @NonNull IOperatorsFactory makeOperatorsFactory(IManyToMany1DDefinition manyToManyDefinition) {

		return new StandardOperatorsFactory() {
			@Override
			public IDecomposition makeDecomposition(String key, Map<String, ?> options) {
				if (ManyToMany1DDecomposition.KEY.equals(key)
						|| key.equals(ManyToMany1DDecomposition.class.getName())) {
					return new ManyToMany1DDecomposition(options, manyToManyDefinition);
				}
				return switch (key) {
				default:
					yield super.makeDecomposition(key, options);
				};
			}
		};
	}

	final String dispatchedMeasure = "k1.dispatched";

	final String cElement = "integer";
	final String cGroup = "integer_groups";

	@Override
	@BeforeEach
	public void feedTable() {
		for (int i = 0; i <= maxElementInserted; i++) {
			table.add(Map.of("l", "A", cElement, i, "k1", elementValue(i)));
		}
	}

	private long elementValue(int i) {
		return LongMath.checkedMultiply(i, i);
	}

	private long elementAggregatedValue(IntStream elementStream) {
		return elementStream.mapToLong(this::elementValue).sum();
	}

	private long groupAggregatedValue(int group) {
		return elementAggregatedValue(groupToElements(group).stream().mapToInt(i -> i));
	}

	void prepareMeasures() {
		Map<String, Object> options = ImmutableMap.<String, Object>builder()
				.put(ManyToMany1DDecomposition.K_INPUT, cElement)
				.put(ManyToMany1DDecomposition.K_OUTPUT, cGroup)
				.build();

		forest.addMeasure(Dispatchor.builder()
				.name(dispatchedMeasure)
				.underlying("k1")
				.decompositionKey(ManyToMany1DDecomposition.class.getName())
				.decompositionOptions(options)
				.build());

		forest.addMeasure(k1Sum);
	}

	@Test
	public void testGrandTotal() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder().measure(dispatchedMeasure).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		// +1 as we include
		long total = elementAggregatedValue(IntStream.rangeClosed(0, maxElementInserted));

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, total));
	}

	@Test
	public void testCountryWithMultipleGroups() {
		prepareMeasures();

		ITabularView output =
				aqw.execute(AdhocQuery.builder().measure(dispatchedMeasure).andFilter(cElement, smallElement).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		long valueSingleElement = elementAggregatedValue(IntStream.of(smallElement));
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, valueSingleElement));
	}

	@Test
	public void testGrandTotal_filterSmallGroup() {
		prepareMeasures();

		ITabularView output =
				aqw.execute(AdhocQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, smallGroup).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, groupAggregatedValue(smallGroup)));
	}

	@Test
	public void test_GroupByElement_FilterOneGroup() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cElement)
				.andFilter(cGroup, smallGroup)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(groupToMaxElement(smallGroup) + 1)
				.containsEntry(Map.of(cElement, 0), Map.of(dispatchedMeasure, elementValue(0)))
				.containsEntry(Map.of(cElement, smallElement), Map.of(dispatchedMeasure, elementValue(smallElement)));
	}

	@Test
	public void test_GroupByElement_FilterMultipleGroups() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cElement)
				.andFilter(cGroup, Set.of(smallGroup, largeGroup))
				// DEBUG is problematic as QueryStep are very large, due to very large InMatcher
				// .debug(false)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(groupToMaxElement(largeGroup) + 1)
				.containsEntry(Map.of(cElement, 0), Map.of(dispatchedMeasure, elementValue(0)))
				.containsEntry(Map.of(cElement, smallElement), Map.of(dispatchedMeasure, elementValue(smallElement)))
				.containsEntry(Map.of(cElement, largeElement), Map.of(dispatchedMeasure, elementValue(largeElement)));
	}

	@Test
	public void test_GroupByGroup_FilterOneElement() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cGroup)
				.andFilter(cElement, smallElement)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				// We filtered 5, so we have groups from 3 to G
				.hasSize(maxGroupCardinality - elementToSmallestGroup(smallElement) + 1)
				.containsEntry(Map.of(cGroup, smallElement), Map.of(dispatchedMeasure, elementValue(smallElement)))
				.containsEntry(Map.of(cGroup, maxGroupCardinality),
						Map.of(dispatchedMeasure, elementValue(smallElement)));
	}

	@Test
	public void test_NoGroupBy_FilterOneGroup() {
		prepareMeasures();

		ITabularView output =
				aqw.execute(AdhocQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, largeGroup).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of(dispatchedMeasure, groupAggregatedValue(largeGroup)));
	}
}
