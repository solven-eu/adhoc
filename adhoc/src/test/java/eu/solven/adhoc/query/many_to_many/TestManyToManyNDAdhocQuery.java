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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.IOperatorsFactory;
import eu.solven.adhoc.measure.StandardOperatorsFactory;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.many2many.IManyToManyNDDefinition;
import eu.solven.adhoc.measure.decomposition.many2many.ManyToMany1DDecomposition;
import eu.solven.adhoc.measure.decomposition.many2many.ManyToManyNDDecomposition;
import eu.solven.adhoc.measure.decomposition.many2many.ManyToManyNDInMemoryDefinition;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;

public class TestManyToManyNDAdhocQuery extends ADagTest implements IAdhocTestConstants {

	ManyToManyNDInMemoryDefinition manyToManyDefinition = new ManyToManyNDInMemoryDefinition();

	public final AdhocQueryEngine aqe =
			editEngine().operatorsFactory(makeOperatorsFactory(manyToManyDefinition)).build();
	public final CubeWrapper aqw = ((ADagTest) this).cube.toBuilder().engine(aqe).build();

	IOperatorsFactory makeOperatorsFactory(IManyToManyNDDefinition manyToManyDefinition) {

		return new StandardOperatorsFactory() {
			@Override
			public IDecomposition makeDecomposition(String key, Map<String, ?> options) {
				if (ManyToMany1DDecomposition.KEY.equals(key)
						|| key.equals(ManyToManyNDDecomposition.class.getName())) {
					return new ManyToManyNDDecomposition(options, manyToManyDefinition);
				}
				return switch (key) {
				default:
					yield super.makeDecomposition(key, options);
				};
			}
		};
	}

	final String dispatchedMeasure = "k1.dispatched";

	final String cElementGender = "gender";
	final String cElementAge = "age";
	final String cGroup = "color";

	@Override
	@BeforeEach
	public void feedTable() {
		manyToManyDefinition.putElementToGroup(Map.of(cElementGender,
				EqualsMatcher.builder().operand("male").build(),
				cElementAge,
				EqualsMatcher.builder().operand("young").build()), Set.of("blue", "yellow"));
		manyToManyDefinition.putElementToGroup(Map.of(cElementGender,
				EqualsMatcher.builder().operand("male").build(),
				cElementAge,
				EqualsMatcher.builder().operand("old").build()), Set.of("blue"));

		manyToManyDefinition.putElementToGroup(Map.of(cElementGender,
				EqualsMatcher.builder().operand("female").build(),
				cElementAge,
				EqualsMatcher.builder().operand("young").build()), Set.of("red", "yellow"));
		manyToManyDefinition.putElementToGroup(Map.of(cElementGender,
				EqualsMatcher.builder().operand("female").build(),
				cElementAge,
				EqualsMatcher.builder().operand("old").build()), Set.of("red"));

		table.add(Map.of("l", "A", cElementGender, "male", cElementAge, "young", "k1", 123));
		table.add(Map.of("l", "A", cElementGender, "male", cElementAge, "old", "k1", 234));
		table.add(Map.of("l", "A", cElementGender, "female", cElementAge, "young", "k1", 345));
		table.add(Map.of("l", "A", cElementGender, "female", cElementAge, "old", "k1", 456));
	}

	void prepareMeasures() {
		Map<String, Object> options = ImmutableMap.<String, Object>builder()
				.put(ManyToManyNDDecomposition.K_INPUTS, Arrays.asList(cElementGender, cElementAge))
				.put(ManyToManyNDDecomposition.K_OUTPUT, cGroup)
				.build();

		forest.addMeasure(Dispatchor.builder()
				.name(dispatchedMeasure)
				.underlying("k1")
				.decompositionKey(ManyToManyNDDecomposition.class.getName())
				.decompositionOptions(options)
				.build());

		forest.addMeasure(k1Sum);
	}

	@Test
	public void testGrandTotal() {
		prepareMeasures();

		ITabularView output = aqw.execute(CubeQuery.builder().measure(dispatchedMeasure).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123 + 234 + 345 + 456));
	}

	@Test
	public void testGrandTotal_filterElement() {
		prepareMeasures();

		ITabularView output =
				aqw.execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cElementGender, "male").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123 + 234));
	}

	@Test
	public void testGrandTotal_filterGroup() {
		prepareMeasures();

		ITabularView output =
				aqw.execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, "yellow").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123 + 345));
	}

	@Test
	public void testGrandTotal_filterGroupWithNoElement() {
		prepareMeasures();

		ITabularView output =
				aqw.execute(CubeQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, "unknownGroup").build());

		Assertions.assertThat(output.isEmpty()).isTrue();
	}

	@Test
	public void test_GroupByPartialElements_FilterOneGroup() {
		prepareMeasures();

		ITabularView output = aqw.execute(CubeQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cElementGender)
				.andFilter(cGroup, "yellow")
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of(cElementGender, "male"), Map.of(dispatchedMeasure, 0L + 123))
				.containsEntry(Map.of(cElementGender, "female"), Map.of(dispatchedMeasure, 0L + 345));
	}

	@Test
	public void test_GroupByAllElement_FilterOneGroup() {
		prepareMeasures();

		ITabularView output = aqw.execute(CubeQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cElementGender, cElementAge)
				.andFilter(cGroup, "yellow")
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of(cElementGender, "male", cElementAge, "young"),
						Map.of(dispatchedMeasure, 0L + 123))
				.containsEntry(Map.of(cElementGender, "female", cElementAge, "young"),
						Map.of(dispatchedMeasure, 0L + 345));
	}

	@Test
	public void test_GroupByPartialElement_FilterMultipleOverlappingGroups() {
		prepareMeasures();

		ITabularView output = aqw.execute(CubeQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cElementGender)
				.andFilter(cGroup, Set.of("yellow", "blue"))
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of(cElementGender, "male"), Map.of(dispatchedMeasure, 0L + 123 + 234))
				.containsEntry(Map.of(cElementGender, "female"), Map.of(dispatchedMeasure, 0L + 345));
	}

	@Test
	public void test_GroupByGroup_noFilter() {
		prepareMeasures();

		ITabularView output = aqw.execute(CubeQuery.builder().measure(dispatchedMeasure).groupByAlso(cGroup).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(3)
				.containsEntry(Map.of(cGroup, "yellow"), Map.of(dispatchedMeasure, 0L + 123 + 345))
				.containsEntry(Map.of(cGroup, "blue"), Map.of(dispatchedMeasure, 0L + 123 + 234))
				.containsEntry(Map.of(cGroup, "red"), Map.of(dispatchedMeasure, 0L + 345 + 456));
	}

	@Test
	public void test_GroupByGroup_FilterPartialElement() {
		prepareMeasures();

		ITabularView output = aqw.execute(CubeQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cGroup)
				.andFilter(cElementGender, "male")
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of(cGroup, "yellow"), Map.of(dispatchedMeasure, 0L + 123))
				.containsEntry(Map.of(cGroup, "blue"), Map.of(dispatchedMeasure, 0L + 123 + 234));
	}

	@Test
	public void test_GroupByGroup_FilterOrDifferentElements() {
		prepareMeasures();

		ITabularView output = aqw.execute(CubeQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cGroup)
				.filter(OrFilter.or(ColumnFilter.isEqualTo(cElementGender, "male"),
						ColumnFilter.isEqualTo(cElementAge, "young")))
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(3)
				.containsEntry(Map.of(cGroup, "yellow"), Map.of(dispatchedMeasure, 0L + 123 + 345))
				.containsEntry(Map.of(cGroup, "blue"), Map.of(dispatchedMeasure, 0L + 123 + 234))
				.containsEntry(Map.of(cGroup, "red"), Map.of(dispatchedMeasure, 0L + 345));
	}

	@Test
	public void test_GroupByGroup_FilterGroup() {
		prepareMeasures();

		ITabularView output = aqw.execute(CubeQuery.builder()
				.measure(dispatchedMeasure)
				.groupByAlso(cGroup)
				.andFilter(cGroup, "yellow")
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(cGroup, "yellow"), Map.of(dispatchedMeasure, 0L + 123 + 345));
	}

}
