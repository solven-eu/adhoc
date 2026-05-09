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
package eu.solven.adhoc.measure.transformator;

import java.util.Collections;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ATestDagInMemory;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.model.measure.Unfiltrator;

/**
 * End-to-end coverage of {@link Unfiltrator} executed through
 * {@link eu.solven.adhoc.measure.transformator.step.UnfiltratorQueryStep}. The contract under test: an
 * {@link Unfiltrator} widens the query filter for its underlying measure, neutralising the constraints on the declared
 * columns (Suppress) or keeping only those constraints (Retain). Useful for hierarchical share-of-total-style measures.
 */
public class TestDagTransformator_Unfiltrator extends ATestDagInMemory implements IAdhocTestConstants {

	@Override
	@BeforeEach
	public void feedTable() {
		table().add(Map.of("a", "a1", "k1", 100));
		table().add(Map.of("a", "a1", "k1", 50));
		table().add(Map.of("a", "a2", "k1", 200));
	}

	// Suppress mode: the unfiltrator removes the filter on `a`, so even though the query filters `a=a1`, the
	// underlying k1 sees the whole table.
	@Test
	public void testUnfiltrator_suppressMode_widensFilter() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(
				Unfiltrator.builder().name("k1_unfilteredOnA").underlying(k1Sum.getName()).column("a").build());

		CubeQuery query = CubeQuery.builder().measure("k1_unfilteredOnA", "k1").andFilter("a", "a1").build();

		ITabularView output = cube().execute(query);
		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		// k1 stays filtered (sum over a=a1 only), k1_unfilteredOnA sees the whole table.
		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(),
						Map.of("k1", 0L + 100 + 50, "k1_unfilteredOnA", 0L + 100 + 50 + 200));
	}

	// Retain mode: only the listed columns keep their filter; everything else is widened. The fluent helper
	// `unfilterOthersThan(...)` is the canonical entry point for this mode.
	@Test
	public void testUnfiltrator_retainMode_keepsOnlyListedColumns() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(
				Unfiltrator.builder().name("k1_retainA").underlying(k1Sum.getName()).unfilterOthersThan("a").build());

		// Filter on both `a` and a different column — only the `a` filter survives in the underlying step.
		CubeQuery query = CubeQuery.builder()
				.measure("k1_retainA")
				.andFilter("a", "a1")
				.andFilter(ColumnFilter.matchEq("a", "a1"))
				.build();

		ITabularView output = cube().execute(query);
		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("k1_retainA", 0L + 100 + 50));
	}

	// matchAll filter case: the unfiltrator must not synthesise an unexpected filter when there is nothing to widen.
	@Test
	public void testUnfiltrator_matchAllFilter_returnsFullAggregate() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(
				Unfiltrator.builder().name("k1_unfilteredOnA").underlying(k1Sum.getName()).column("a").build());

		ITabularView output = cube().execute(CubeQuery.builder().measure("k1_unfilteredOnA").build());
		MapBasedTabularView mapBased = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBased.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Collections.emptyMap(), Map.of("k1_unfilteredOnA", 0L + 100 + 50 + 200));
	}

}
