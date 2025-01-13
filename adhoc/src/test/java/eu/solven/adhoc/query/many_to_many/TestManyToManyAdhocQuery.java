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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.ITabularView;
import eu.solven.adhoc.MapBasedTabularView;
import eu.solven.adhoc.aggregations.IDecomposition;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.aggregations.StandardOperatorsFactory;
import eu.solven.adhoc.aggregations.many_to_many.IManyToManyDefinition;
import eu.solven.adhoc.aggregations.many_to_many.ManyToManyDecomposition;
import eu.solven.adhoc.aggregations.many_to_many.ManyToManyInMemoryDefinition;
import eu.solven.adhoc.dag.AdhocQueryEngine;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.transformers.Dispatchor;
import lombok.NonNull;

public class TestManyToManyAdhocQuery extends ADagTest implements IAdhocTestConstants {

	ManyToManyInMemoryDefinition manyToManyDefinition = new ManyToManyInMemoryDefinition();

	public final AdhocQueryEngine aqe = AdhocQueryEngine.builder()
			.eventBus(eventBus)
			.measureBag(amb)
			.operatorsFactory(makeOperatorsFactory(manyToManyDefinition))
			.build();

	private @NonNull IOperatorsFactory makeOperatorsFactory(IManyToManyDefinition manyToManyDefinition) {

        return new StandardOperatorsFactory() {
            @Override
            public IDecomposition makeDecomposition(String key, Map<String, ?> options) {
                if (ManyToManyDecomposition.KEY.equals(key) || key.equals(ManyToManyDecomposition.class.getName())) {
                    return new ManyToManyDecomposition(options, manyToManyDefinition);
                }
                return switch (key) {
                    default:
                        yield super.makeDecomposition(key, options);
                };
            }
        };
    }

	final String dispatchedMeasure = "k1.dispatched";

	final String cElement = "country";
	final String cGroup = "country_groups";

	@Override
	@BeforeEach
	public void feedDb() {
		manyToManyDefinition.putElementToGroup("FR", "G8");
		manyToManyDefinition.putElementToGroup("FR", "G20");
		manyToManyDefinition.putElementToGroup("CH", "G20");

		rows.add(Map.of("l", "A", cElement, "FR", "k1", 123));
		rows.add(Map.of("l", "A", cElement, "CH", "k1", 234));
		rows.add(Map.of("l", "A", cElement, "ZW", "k1", 345));
	}

	void prepareMeasures() {
		Map<String, Object> options = ImmutableMap.<String, Object>builder()
				.put(ManyToManyDecomposition.K_INPUT, cElement)
				.put(ManyToManyDecomposition.K_OUTPUT, cGroup)
				.build();

		amb.addMeasure(Dispatchor.builder()
				.name(dispatchedMeasure)
				.underlying("k1")
				.decompositionKey(ManyToManyDecomposition.class.getName())
				.decompositionOptions(options)
				.build());

		amb.addMeasure(k1Sum);
	}

	@Test
	public void testGrandTotal() {
		prepareMeasures();

		ITabularView output = aqe.execute(AdhocQuery.builder().measure(dispatchedMeasure).build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123 + 234 + 345));
	}

	@Test
	public void testCountryWithMultipleGroups() {
		prepareMeasures();

		ITabularView output =
				aqe.execute(AdhocQuery.builder().measure(dispatchedMeasure).andFilter(cElement, "FR").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123));
	}

	@Test
	public void testCountryWithSingleGroups() {
		prepareMeasures();

		ITabularView output =
				aqe.execute(AdhocQuery.builder().measure(dispatchedMeasure).andFilter(cElement, "CH").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 234));
	}

	@Test
	public void testCountryWithNoGroup() {
		prepareMeasures();

		ITabularView output =
				aqe.execute(AdhocQuery.builder().measure(dispatchedMeasure).andFilter(cElement, "CH").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 234));
	}

	@Test
	public void testGroupWithMultipleCountry() {
		prepareMeasures();

		ITabularView output =
				aqe.execute(AdhocQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, "G20").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123 + 234));
	}

	@Test
	public void testGroupWithSingleCountry() {
		prepareMeasures();

		ITabularView output =
				aqe.execute(AdhocQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, "G8").build(), rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(1).contains(Collections.emptyMap());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(dispatchedMeasure, 0L + 123));
	}

	@Test
	public void testGroupWithNoCountry() {
		prepareMeasures();

		ITabularView output =
				aqe.execute(AdhocQuery.builder().measure(dispatchedMeasure).andFilter(cGroup, "unknownGroup").build(),
						rows);

		List<Map<String, ?>> keySet = output.keySet().collect(Collectors.toList());
		Assertions.assertThat(keySet).hasSize(0);
	}

	@Test
	public void test_GroupByElement_FilterOneGroup() {
		prepareMeasures();

		ITabularView output = aqe.execute(AdhocQuery.builder()
				.measure(dispatchedMeasure)
				.groupByColumns(cElement)
				.andFilter(cGroup, "G8")
				.build(), rows);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(cElement, "FR"), Map.of(dispatchedMeasure, 0L + 123));
	}

	@Test
	public void test_GroupByElement_FilterMultipleGroups() {
		prepareMeasures();

		ITabularView output = aqe.execute(AdhocQuery.builder()
				.measure(dispatchedMeasure)
				.groupByColumns(cElement)
				.andFilter(cGroup, Set.of("G8", "G20"))
				.build(), rows);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of(cElement, "FR"), Map.of(dispatchedMeasure, 0L + 123))
				.containsEntry(Map.of(cElement, "CH"), Map.of(dispatchedMeasure, 0L + 234));
	}

	@Test
	public void test_GroupByGroup_FilterOneElement() {
		prepareMeasures();

		ITabularView output = aqe.execute(AdhocQuery.builder()
				.measure(dispatchedMeasure)
				.groupByColumns(cGroup)
				.andFilter(cElement, "FR")
				.build(), rows);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of(cGroup, "G8"), Map.of(dispatchedMeasure, 0L + 123))
				.containsEntry(Map.of(cGroup, "G20"), Map.of(dispatchedMeasure, 0L + 123));
	}

	@Test
	public void test_GroupByGroup_FilterMultipleElements() {
		prepareMeasures();

		ITabularView output = aqe.execute(AdhocQuery.builder()
				.measure(dispatchedMeasure)
				.groupByColumns(cGroup)
				.andFilter(cElement, Set.of("FR", "CH"))
				.build(), rows);

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of(cGroup, "G8"), Map.of(dispatchedMeasure, 0L + 123))
				.containsEntry(Map.of(cGroup, "G20"), Map.of(dispatchedMeasure, 0L + 123 + 234));
	}
}
