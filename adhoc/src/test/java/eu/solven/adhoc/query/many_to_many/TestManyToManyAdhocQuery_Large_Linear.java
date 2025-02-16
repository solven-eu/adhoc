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
import java.util.HashMap;
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

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.AdhocCubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.many2many.IManyToMany1DDefinition;
import eu.solven.adhoc.measure.decomposition.many2many.ManyToMany1DDecomposition;
import eu.solven.adhoc.measure.step.Dispatchor;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.storage.ITabularView;
import eu.solven.adhoc.storage.MapBasedTabularView;
import lombok.NonNull;

/**
 * These unitTests are dedicated to check ManyToMany perforances in case of large problems. We can encounter millions of
 * elements/groups and groups of millions of elements in real-life.
 * <p>
 * Here, we consider groups with quadratic growth: n-th group contains n elements. It is useful to check performance
 * when we have many not very small groups
 */
public class TestManyToManyAdhocQuery_Large_Linear extends ADagTest implements IAdhocTestConstants {
	// This could be adjusted so that tests takes a few seconds to execute. Can be increased a lot to test bigger cases
	int maxCardinality = 0_100_000;

	int smallValue = 5;
	int largeValue = maxCardinality - smallValue;

	IManyToManyGroupToElements groupToElements = new IManyToManyGroupToElements() {
		private IntStream streamMatchingGroups(IValueMatcher groupMatcher) {
			return IntStream.rangeClosed(0, maxCardinality).filter(groupMatcher::match);
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
	// ...
	// group=N -> element=0|N
	// which implies
	// element=0 -> group=0|N
	// element=1 -> group=1|N
	// ...
	// element=N-1 -> group=N-1|N
	// element=N -> group=N
	IManyToMany1DDefinition manyToManyDefinition = ManyToMany1DDynamicDefinition.builder().elementToGroups(element -> {
		if (element instanceof Integer asInt) {
			if (asInt < 0) {
				return Set.of(-1);
			} else if (asInt > maxCardinality) {
				return Set.of(maxCardinality + 1);
			}
			return Collections.unmodifiableSet(
					ContiguousSet.create(Range.closed(asInt, maxCardinality), DiscreteDomain.integers()));
		} else {
			return Set.of(element);
		}
	}).groupToElements(groupToElements).build();

	private Set<Integer> groupToElements(int group) {
		if (group < 0) {
			// Beware, it means we do not list all negative integers as elements of -1
			return Set.of(-1);
		} else if (group > maxCardinality) {
			// Beware, it means we do not list all integers larger than maxCardinality as elements of maxCardinality
			return Set.of(maxCardinality + 1);
		} else {
			return ContiguousSet.create(Range.closed(0, group), DiscreteDomain.integers());
		}
	}

	public final AdhocQueryEngine aqe =
			editEngine().operatorsFactory(makeOperatorsFactory(manyToManyDefinition)).build();
	public final AdhocCubeWrapper aqw = AdhocCubeWrapper.builder().table(rows).engine(aqe).measures(amb).build();

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
		for (int i = 0; i < maxCardinality; i++) {
			rows.add(Map.of("l", "A", cElement, i, "k1", elementValue(i)));
		}
	}

	private long elementValue(int i) {
		return 1L * i * i;
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

		amb.addMeasure(Dispatchor.builder()
				.name(dispatchedMeasure)
				.underlying("k1")
				.decompositionKey(ManyToMany1DDecomposition.class.getName())
				.decompositionOptions(options)
				.build());

		amb.addMeasure(k1Sum);
	}

	@Test
	public void testGrandTotal() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder().measure(dispatchedMeasure).build());

		// List<Map<String, ?>> keySet =
		// output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		// Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		long total = elementAggregatedValue(IntStream.range(0, maxCardinality));

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, total));
	}

	@Test
	public void testCountryWithMultipleGroups() {
		prepareMeasures();

		ITabularView output =
				aqw.execute(AdhocQuery.builder().measure(dispatchedMeasure).andFilter(cElement, smallValue).build());

		// List<Map<String, ?>> keySet =
		// output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		// Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		long valueSingleElement = elementAggregatedValue(IntStream.of(smallValue));
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, valueSingleElement));
	}

	@Test
	public void testGrandTotal_filterSmallGroup() {
		prepareMeasures();

		ITabularView output =
				aqw.execute(AdhocQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, smallValue).build());

		// List<Map<String, ?>> keySet =
		// output.keySet().map(AdhocSliceAsMap::getCoordinates).collect(Collectors.toList());
		// Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, groupAggregatedValue(smallValue)));
	}

	@Test
	public void test_GroupByElement_FilterOneGroup() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cElement)
				.andFilter(cGroup, smallValue)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(smallValue + 1)
				.containsEntry(Map.of(cElement, 0), Map.of(dispatchedMeasure, elementValue(0)))
				.containsEntry(Map.of(cElement, smallValue), Map.of(dispatchedMeasure, elementValue(smallValue)));
	}

	@Test
	public void test_GroupByElement_FilterMultipleGroups() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cElement)
				.andFilter(cGroup, Set.of(smallValue, largeValue))
				// DEBUG is problematic as QueryStep are very large, due to very large InMatcher
				// .debug(true)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(largeValue + 1)
				.containsEntry(Map.of(cElement, 0), Map.of(dispatchedMeasure, elementValue(0)))
				.containsEntry(Map.of(cElement, smallValue), Map.of(dispatchedMeasure, elementValue(smallValue)))
				.containsEntry(Map.of(cElement, largeValue), Map.of(dispatchedMeasure, elementValue(largeValue)));
	}

	@Test
	public void test_GroupByGroup_FilterOneElement() {
		prepareMeasures();

		ITabularView output = aqw.execute(AdhocQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cGroup)
				.andFilter(cElement, smallValue)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output,
				MapBasedTabularView.builder()
						// HashMap as this is a performance test
						.coordinatesToValues(new HashMap<>(output.size()))
						.build());

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				// We filtered 5, so we have groups from 5 to N
				.hasSize(largeValue + 1)
				.containsEntry(Map.of(cGroup, smallValue), Map.of(dispatchedMeasure, elementValue(smallValue)))
				.containsEntry(Map.of(cGroup, maxCardinality), Map.of(dispatchedMeasure, elementValue(smallValue)));
	}

	@Test
	public void test_NoGroupBy_FilterOneGroup() {
		prepareMeasures();

		ITabularView output =
				aqw.execute(AdhocQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, largeValue).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of(dispatchedMeasure, groupAggregatedValue(largeValue)));
	}
}
