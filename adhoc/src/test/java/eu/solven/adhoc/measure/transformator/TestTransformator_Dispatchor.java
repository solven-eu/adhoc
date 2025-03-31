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

import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.cube.AdhocQuery;

public class TestTransformator_Dispatchor extends ADagTest implements IAdhocTestConstants {
	@Override
	@BeforeEach
	public void feedTable() {
		table.add(Map.of("a", "a1", "percent", 1, "k1", 123));
		table.add(Map.of("a", "a1", "percent", 10, "k1", 345, "k2", 456));
		table.add(Map.of("a", "a2", "percent", 100, "b", "b1", "k2", 234));
		table.add(Map.of("a", "a2", "percent", 50, "b", "b2", "k1", 567));
	}

	@Test
	public void testSumOfMaxOfSum_identity() {
		forest.addMeasure(
				Dispatchor.builder().name("0or100").underlying("k1").aggregationKey(SumAggregation.KEY).build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube.execute(AdhocQuery.builder().measure("0or100").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("0or100", 0L + (123 + 345) + 567));
	}

	@Test
	public void testSumOfMaxOfSum_0to100_inputNotRequested() {
		forest.addMeasure(Dispatchor.builder()
				.name("0or100")
				.underlying("k1")
				.decompositionKey("linear")
				.decompositionOptions(Map.of("input", "percent", "min", 0, "max", 100, "output", "0_or_100"))
				.aggregationKey(SumAggregation.KEY)
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output = cube.execute(AdhocQuery.builder().measure("0or100").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("0or100", 0L + (123 + 345) + 567));
	}

	@Test
	public void testSumOfMaxOfSum_0to100() {
		forest.addMeasure(Dispatchor.builder()
				.name("0or100")
				.underlying("k1")
				// .combinatorKey(MaxTransformation.KEY)
				.decompositionKey("linear")
				.decompositionOptions(Map.of("input", "percent", "min", 0, "max", 100, "output", "0_or_100"))
				.aggregationKey(SumAggregation.KEY)
				.tag("debug")
				.build());

		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		ITabularView output =
				cube.execute(AdhocQuery.builder().measure("0or100").groupByAlso("0_or_100").explain(true).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("0_or_100", 0), Map.of("0or100", 0D + 123 * 0.01D + 345 * .1D + 567 * 0.5D))
				.containsEntry(Map.of("0_or_100", 100), Map.of("0or100", 0D + 123 * 0.99D + 345 * 0.9D + 567 * 0.5D));
	}

}
