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
package eu.solven.adhoc.query;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.dag.context.DefaultQueryPreparator;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.combination.FindFirstCombination;
import eu.solven.adhoc.measure.model.Bucketor;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestQueryOption_Concurrent extends ADagTest implements IAdhocTestConstants {

	@Override
	@BeforeEach
	public void feedTable() {
		table.add(Map.of("k", "a"));
		table.add(Map.of("k", "b"));
		table.add(Map.of("k", "c"));

		forest.addMeasure(countAsterisk);

		forest.addMeasure(Bucketor.builder()
				.name("byK")
				.underlying(countAsterisk.getName())
				.groupBy(GroupByColumns.named("k"))
				.combinationKey(FindFirstCombination.KEY)
				.build());
	}

	@Test
	public void testGrandTotal() {
		CubeWrapper cube = editCube().queryPreparator(
				DefaultQueryPreparator.builder().implicitOptions(q -> Set.of(StandardQueryOptions.CONCURRENT)).build())
				.build();

		ITabularView output = cube.execute(AdhocQuery.builder().measure(countAsterisk.getName()).build());

		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of(countAsterisk.getName(), 3L));
	}

}
