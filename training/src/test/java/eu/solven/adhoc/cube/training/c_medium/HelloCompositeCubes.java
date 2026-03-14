/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.cube.training.c_medium;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.forest.MeasureForest;
import eu.solven.adhoc.measure.forest.UnsafeMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.table.InMemoryTable;
import eu.solven.adhoc.table.composite.CompositeCubesTableWrapper;

public class HelloCompositeCubes {

	@Test
	public void helloCompositeCube() {
		ICubeWrapper cube1;
		{
			InMemoryTable table1 = InMemoryTable.builder().build();
			table1.add(ImmutableMap.of("a", "a1", "b", "b1", "v1", 1, "v2", 2));
			table1.add(ImmutableMap.of("a", "a1", "b", "b2", "v2", 7));
			table1.add(ImmutableMap.of("a", "a2", "b", "b1", "v1", 11));

			MeasureForest forest1 = MeasureForest.builder()
					.name("someForest")
					.measure(Aggregator.sum("v1"))
					.measure(Aggregator.sum("v2"))
					.build();

			cube1 = CubeWrapper.builder().name("cube1").table(table1).forest(forest1).build();
		}

		ICubeWrapper cube2;
		{
			InMemoryTable table2 = InMemoryTable.builder().build();
			table2.add(ImmutableMap.of("a", "a1", "c", "c1", "v1", 17, "v3", 22));
			table2.add(ImmutableMap.of("a", "a1", "c", "c2", "v3", 27));
			table2.add(ImmutableMap.of("a", "a2", "c", "c1", "v1", 31));

			MeasureForest forest2 = MeasureForest.builder()
					.name("someForest")
					.measure(Aggregator.sum("v1"))
					.measure(Aggregator.sum("v3"))
					.build();

			cube2 = CubeWrapper.builder().name("cube2").table(table2).forest(forest2).build();
		}

		CompositeCubesTableWrapper compositeTable =
				CompositeCubesTableWrapper.builder().cube(cube1).cube(cube2).build();

		// Composite has the union of columns
		Assertions.assertThat(compositeTable.getColumns())
				.hasSize(7)
				// columns from cube1
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("a"))
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("b"))
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("v1"))
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("v2"))
				// additional columns from cube2
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("c"))
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("v3"))
				// a special column enabling to differentiate the underlying cubes
				.anySatisfy(c -> Assertions.assertThat(c.getName()).isEqualTo("~CompositeSlicer"));

		UnsafeMeasureForest withoutUnderlyings = UnsafeMeasureForest.builder().name("composite").build();

		IMeasure sumV2V3 = Combinator.sum("v2", "v3");
		withoutUnderlyings.addMeasure(sumV2V3);

		IMeasureForest withUnderlyings = compositeTable.injectUnderlyingMeasures(withoutUnderlyings);

		ICubeWrapper cubeComposite = CubeWrapper.builder().table(compositeTable).forest(withUnderlyings).build();

		ICubeQuery query = CubeQuery.builder().groupByAlso("a").measure("v1", "v2", "v3", sumV2V3.getName()).build();

		ITabularView result = cubeComposite.execute(query);
		MapBasedTabularView resultListBased = MapBasedTabularView.load(result);

		Assertions.assertThat(resultListBased.getCoordinatesToValues())
				.hasSize(2)
				.containsEntry(Map.of("a", "a1"),
						Map.of("v1", 1L + 17L, "v2", 2L + 7L, "v3", 22L + 27L, sumV2V3.getName(), 2L + 7L + 22L + 27L))
				.containsEntry(Map.of("a", "a2"), Map.of("v1", 11L + 31L));
	}
}
