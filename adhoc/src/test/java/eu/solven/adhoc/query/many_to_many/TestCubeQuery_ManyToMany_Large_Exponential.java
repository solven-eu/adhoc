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
import java.util.stream.LongStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.AdhocFactories;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.many2many.IManyToMany1DDefinition;
import eu.solven.adhoc.measure.decomposition.many2many.ManyToMany1DDecomposition;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * These unitTests are dedicated to check ManyToMany performances in case of large problems. We can encounter millions
 * of elements/groups and groups of millions of elements in real-life.
 * <p>
 * Here, we consider groups with exponential growth: n-th group contains 2^n elements. It is useful to check performance
 * when we have many not very small groups
 */
@Slf4j
public class TestCubeQuery_ManyToMany_Large_Exponential extends ADagTest implements IAdhocTestConstants {
	// This could be adjusted so that tests takes a few seconds to execute. Can be increased a lot to test bigger cases
	static int maxGroupCardinality = 18;
	static int maxElementCardinality = groupToMaxElement(maxGroupCardinality);

	// +1 to also write a value which is larger than the largest element of the largest group
	int maxElementInserted = maxElementCardinality + 2;

	private static int groupToMaxElement(int group) {
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
	int largeElement = Ints.checkedCast(maxElementCardinality - smallElement);

	int smallGroup = elementToSmallestGroup(smallElement);
	int largeGroup = elementToSmallestGroup(largeElement);

	IManyToManyGroupToElements groupToElements = new IManyToManyGroupToElements() {
		private LongStream streamMatchingGroups(IValueMatcher groupMatcher) {
			return LongStream.rangeClosed(0, maxGroupCardinality).filter(groupMatcher::match);
		}

		@Override
		public Set<?> getElementsMatchingGroups(IValueMatcher groupMatcher) {
			if (groupMatcher instanceof EqualsMatcher equalsMatcher
					&& AdhocPrimitiveHelpers.isLongLike(equalsMatcher.getOperand())) {
				long group = AdhocPrimitiveHelpers.asLong(equalsMatcher.getOperand());
				return groupToElementsAsSet(group);
			} else {

				// Sets.union(null, null)
				return streamMatchingGroups(groupMatcher).flatMap(group -> {
					return groupToElements(group);
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
	IManyToMany1DDefinition manyToManyDefinition = ManyToMany1DDynamicDefinition.builder().elementToGroups(element -> {
		if (AdhocPrimitiveHelpers.isLongLike(element)) {
			long asInt = AdhocPrimitiveHelpers.asLong(element);
			if (asInt < 0) {
				return Set.of(-1L);
			} else if (asInt > maxElementCardinality) {
				return Set.of(maxElementCardinality + 1L);
			}
			return Collections.unmodifiableSet(
					ContiguousSet.closed(0L + elementToSmallestGroup(Ints.checkedCast(asInt)), maxElementCardinality));
		} else {
			return Set.of(element);
		}
	}).groupToElements(groupToElements).build();

	private LongStream groupToElements(long group) {
		if (group < 0) {
			// Beware, it means we do not list all negative integers as elements of -1
			return LongStream.empty();
		} else if (group > maxGroupCardinality) {
			// Beware, it means we do not list all integers larger than maxCardinality as elements of maxCardinality
			return LongStream.empty();
		} else {
			return LongStream.rangeClosed(0L, groupToMaxElement(Ints.checkedCast(group)));
		}
	}

	private Set<Long> groupToElementsAsSet(long group) {
		if (group < 0) {
			// Beware, it means we do not list all negative integers as elements of -1
			return Set.of();
		} else if (group > maxGroupCardinality) {
			// Beware, it means we do not list all integers larger than maxCardinality as elements of maxCardinality
			return Set.of();
		} else {
			return Collections.unmodifiableSet(ContiguousSet.closed(0L, groupToMaxElement(Ints.saturatedCast(group))));
		}
	}

	@Override
	public AdhocFactories makeFactories() {
		return super.makeFactories().toBuilder().operatorFactory(makeOperatorsFactory(manyToManyDefinition)).build();
	}

	private @NonNull IOperatorFactory makeOperatorsFactory(IManyToMany1DDefinition manyToManyDefinition) {

		return new StandardOperatorFactory() {
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

	@BeforeAll
	public static void logCardinality() {
		log.info("cardinality groups={}, elements={}", maxGroupCardinality, maxElementCardinality);
	}

	@Override
	@BeforeEach
	public void feedTable() {
		for (int i = 0; i <= maxElementInserted; i++) {
			table().add(Map.of("l", "A", cElement, i, "k1", elementValue(i)));
		}
	}

	private long elementValue(long i) {
		return LongMath.checkedMultiply(i, i);
	}

	private long elementAggregatedValue(LongStream elementStream) {
		return elementStream.map(this::elementValue).sum();
	}

	private long groupAggregatedValue(int group) {
		return elementAggregatedValue(groupToElements(group));
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

		ITabularView output = cube().execute(CubeQuery.builder().measure(dispatchedMeasure).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		// +1 as we include
		long total = elementAggregatedValue(LongStream.rangeClosed(0, maxElementInserted));

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, total));
	}

	@Test
	public void testCountryWithMultipleGroups() {
		prepareMeasures();

		ITabularView output = cube()
				.execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cElement, smallElement).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		long valueSingleElement = elementAggregatedValue(LongStream.of(smallElement));
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, valueSingleElement));
	}

	@Test
	public void testGrandTotal_filterSmallGroup() {
		prepareMeasures();

		ITabularView output =
				cube().execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, smallGroup).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, groupAggregatedValue(smallGroup)));
	}

	@Test
	public void test_GroupByElement_FilterOneGroup() {
		prepareMeasures();

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cElement)
				.andFilter(cGroup, smallGroup)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(groupToMaxElement(smallGroup) + 1)
				.containsEntry(Map.of(cElement, 0L), Map.of(dispatchedMeasure, elementValue(0)))
				.containsEntry(Map.of(cElement, 0L + smallElement),
						Map.of(dispatchedMeasure, elementValue(smallElement)));
	}

	@Test
	public void test_GroupByElement_FilterMultipleGroups() {
		prepareMeasures();

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cElement)
				.andFilter(cGroup, Set.of(smallGroup, largeGroup))
				// DEBUG is problematic as QueryStep are very large, due to very large InMatcher
				// .debug(false)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(groupToMaxElement(largeGroup) + 1)
				.containsEntry(Map.of(cElement, 0L), Map.of(dispatchedMeasure, elementValue(0)))
				.containsEntry(Map.of(cElement, 0L + smallElement),
						Map.of(dispatchedMeasure, elementValue(smallElement)))
				.containsEntry(Map.of(cElement, 0L + largeElement),
						Map.of(dispatchedMeasure, elementValue(largeElement)));
	}

	@Test
	public void test_GroupByGroup_FilterOneElement() {
		prepareMeasures();

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cGroup)
				.andFilter(cElement, smallElement)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				// We filtered 5, so we have groups from 5 to max
				.hasSize(maxGroupCardinality - elementToSmallestGroup(smallElement) + 1)
				.containsEntry(Map.of(cGroup, 0L + smallElement), Map.of(dispatchedMeasure, elementValue(smallElement)))
				.containsEntry(Map.of(cGroup, 0L + maxGroupCardinality),
						Map.of(dispatchedMeasure, elementValue(smallElement)));
	}

	@Test
	public void test_NoGroupBy_FilterOneGroup() {
		prepareMeasures();

		ITabularView output =
				cube().execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, largeGroup).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of(dispatchedMeasure, groupAggregatedValue(largeGroup)));
	}
}
