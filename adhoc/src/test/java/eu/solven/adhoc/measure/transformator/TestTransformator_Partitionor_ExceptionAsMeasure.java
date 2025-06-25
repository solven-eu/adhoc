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
package eu.solven.adhoc.measure.transformator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestTransformator_Partitionor_ExceptionAsMeasure extends ADagTest implements IAdhocTestConstants {
	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("a", "a1", "k1", 123));
		table().add(Map.of("a", "a1", "k1", 345, "k2", 456));
		table().add(Map.of("a", "a2", "b", "b1", "k2", 234));
		table().add(Map.of("a", "a2", "b", "b2", "k1", 567));
	}

	public static class ThrowingCombination implements ICombination {
		@Override
		public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
			throw new IllegalStateException(
					"Simulate a failure slice=%s".formatted(slice.getAdhocSliceAsMap().getCoordinates()));
		}
	}

	String mName = "partitionOnErrors";

	@Test
	public void testGrandTotal_noPartitions() {
		forest.addMeasure(Partitionor.builder()
				.name(mName)
				.underlyings(Arrays.asList("k1", "k2"))
				.combinationKey(ThrowingCombination.class.getName())
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(
				CubeQuery.builder().measure(mName).option(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), measures -> {
			Assertions.assertThat(measures).hasEntrySatisfying(mName, v -> {
				Assertions.assertThat(v)
						.isInstanceOfSatisfying(IllegalArgumentException.class,
								e -> Assertions.assertThat(e)
										.hasRootCause(new IllegalStateException("Simulate a failure slice={}")));
			});
		});
	}

	@Test
	public void testGrandTotal() {
		forest.addMeasure(Partitionor.builder()
				.name(mName)
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a"))
				.combinationKey(ThrowingCombination.class.getName())
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(
				CubeQuery.builder().measure(mName).option(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues()).hasSize(1).hasEntrySatisfying(Map.of(), measures -> {
			Assertions.assertThat(measures).hasEntrySatisfying(mName, v -> {
				Assertions.assertThat(v)
						.isInstanceOfSatisfying(IllegalArgumentException.class,
								e -> Assertions.assertThat(e)
										.hasRootCause(new IllegalStateException("Simulate a failure slice={a=a1}")));
			});
		});
	}

	@Test
	public void testGroupByA() {
		forest.addMeasure(Partitionor.builder()
				.name(mName)
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a"))
				.combinationKey(ThrowingCombination.class.getName())
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(mName)
				.groupByAlso("a")
				.option(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.hasEntrySatisfying(Map.of("a", IllegalArgumentException.class.getName()), measures -> {
					Assertions.assertThat(measures).hasEntrySatisfying(mName, v -> {
						Assertions.assertThat(v)
								.isInstanceOfSatisfying(IllegalArgumentException.class,
										e -> Assertions.assertThat(e)
												.hasRootCause(
														new IllegalStateException("Simulate a failure slice={a=a1}")));
					});
				});
	}

	@Test
	public void testGroupByA_filterA() {
		forest.addMeasure(Partitionor.builder()
				.name(mName)
				.underlyings(Arrays.asList("k1", "k2"))
				.groupBy(GroupByColumns.named("a"))
				.combinationKey(ThrowingCombination.class.getName())
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube().execute(CubeQuery.builder()
				.measure(mName)
				.groupByAlso("a")
				.andFilter("a", "a1")
				.option(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE)
				.build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.hasEntrySatisfying(Map.of("a", "a1"), measures -> {
					Assertions.assertThat(measures).hasEntrySatisfying(mName, v -> {
						Assertions.assertThat(v)
								.isInstanceOfSatisfying(IllegalArgumentException.class,
										e -> Assertions.assertThat(e)
												.hasRootCause(
														new IllegalStateException("Simulate a failure slice={a=a1}")));
					});
				});
	}

}
