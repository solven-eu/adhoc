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
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.model.Columnator;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.query.cube.CubeQuery;

public class TestTransformator_Columnator extends ADagTest implements IAdhocTestConstants {
	Columnator requireC = Columnator.builder()
			.name("requireC")
			.column("c")
			.underlyings(Arrays.asList("k1", "k2"))
			.combinationKey(SumCombination.KEY)
			.build();
	Columnator requireCandD = Columnator.builder()
			.name("requireCandD")
			.column("c")
			.column("d")
			.underlyings(Arrays.asList("k1", "k2"))
			.combinationKey(SumCombination.KEY)
			.build();

	@Override
	@BeforeEach
	public void feedTable() {
		table.add(Map.of("c", "c1", "d", "d1", "k1", 123D));
		table.add(Map.of("c", "c2", "d", "d1", "k2", 234D));
		table.add(Map.of("c", "c2", "d", "d2", "k1", 345F, "k2", 456F));
	}

	@BeforeEach
	public void feedForest() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(k2Sum);

		forest.addMeasure(requireC);
		forest.addMeasure(requireCandD);
	}

	@Test
	public void testGrandTotal_c() {
		ITabularView output = cube().execute(CubeQuery.builder().measure("requireC").build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues()).isEmpty();
	}

	@Test
	public void testGrandTotal_cd() {
		ITabularView output = cube().execute(CubeQuery.builder().measure("requireCandD").build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues()).isEmpty();
	}

	@Test
	public void testGroupByC_c() {
		ITabularView output = cube().execute(CubeQuery.builder().measure("requireC").groupByAlso("c").build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("c", "c1"), Map.of("requireC", 0D + 123));
	}

	@Test
	public void testGroupByC_cd() {
		ITabularView output = cube().execute(CubeQuery.builder().measure("requireCandD").groupByAlso("c").build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues()).isEmpty();
	}

	@Test
	public void testGroupByCD_c() {
		ITabularView output = cube().execute(CubeQuery.builder().measure("requireC").groupByAlso("c", "d").build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(3)
				.containsEntry(Map.of("c", "c1", "d", "d1"), Map.of("requireC", 0D + 123))
				.containsEntry(Map.of("c", "c2", "d", "d1"), Map.of("requireC", 0D + 234))
				.containsEntry(Map.of("c", "c2", "d", "d2"), Map.of("requireC", 0D + 345 + 456));
	}

	@Test
	public void testGroupByCD_cd() {
		ITabularView output = cube().execute(CubeQuery.builder().measure("requireCandD").groupByAlso("c", "d").build());

		Assertions.assertThat(MapBasedTabularView.load(output).getCoordinatesToValues())
				.hasSize(3)
				.containsEntry(Map.of("c", "c1", "d", "d1"), Map.of("requireCandD", 0D + 123))
				.containsEntry(Map.of("c", "c2", "d", "d1"), Map.of("requireCandD", 0D + 234))
				.containsEntry(Map.of("c", "c2", "d", "d2"), Map.of("requireCandD", 0D + 345 + 456));
	}
}
