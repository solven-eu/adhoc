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
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.query.cube.CubeQuery;

/**
 * Checks the behavior of a {@link Combinator} with no explicit underlyings.
 * 
 * @author Benoit Lacelle
 */
public class TestTransformator_Combinator_NoUnderlyings extends ADagTest implements IAdhocTestConstants {
	@BeforeEach
	@Override
	public void feedTable() {
		table().add(Map.of("k1", 123));
		table().add(Map.of("k2", 234));
		table().add(Map.of("k1", 345, "k2", 456));

		forest.addMeasure(
				Combinator.builder().name("constant").combinationKey(ConstantCombinator.class.getName()).build());
	}

	public static class ConstantCombinator implements ICombination {
		@Override
		public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
			return "someConstant";
		}
	}

	@Test
	public void testGrandTotal() {
		ITabularView output = cube().execute(CubeQuery.builder().measure("constant").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("constant", "someConstant"));
	}

	@Test
	public void testGroupByK1() {
		ITabularView output = cube().execute(CubeQuery.builder().measure("constant").groupByAlso("k1").build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.containsEntry(Map.of("k1", 123L), Map.of("constant", "someConstant"))
				.containsEntry(Map.of("k1", 345L), Map.of("constant", "someConstant"))
				.containsEntry(MapWithNulls.of("k1", null), Map.of("constant", "someConstant"))
				.hasSize(3);
	}
}
