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
package eu.solven.adhoc.view;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.query.StandardQueryOptions;
import eu.solven.adhoc.query.cube.AdhocQuery;

public class TestQueryOptions extends ADagTest implements IAdhocTestConstants {
	@BeforeEach
	@Override
	public void feedTable() {
		table.add(Map.of("k1", 123));
		table.add(Map.of("k2", 234));
		table.add(Map.of("k1", 345, "k2", 456));
	}

	@Test
	public void testUnknownMeasuresAreEmpty_direct() {
		forest.addMeasure(k1Sum);

		AdhocQuery query = AdhocQuery.builder().measure("k2").build();

		// By default, an exception is thrown
		Assertions.assertThatThrownBy(() -> engine.executeUnsafe(query, forest, table))
				.isInstanceOf(IllegalArgumentException.class);

		ITabularView output = engine.executeUnsafe(
				AdhocQuery.edit(query).option(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY).build(),
				forest,
				table,
				ColumnsManager.builder().build());

		Assertions.assertThat(output.isEmpty()).isTrue();
	}

	@Test
	public void testUnknownMeasuresAreEmpty_indirect() {
		forest.addMeasure(k1Sum);
		forest.addMeasure(k1PlusK2AsExpr);

		AdhocQuery query = AdhocQuery.builder().measure(k1PlusK2AsExpr).build();

		// By default, an exception is thrown
		Assertions.assertThatThrownBy(() -> engine.executeUnsafe(query, forest, table))
				.isInstanceOf(IllegalArgumentException.class);

		ITabularView output = engine.executeUnsafe(
				AdhocQuery.edit(query).option(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY).build(),
				forest,
				table,
				ColumnsManager.builder().build());

		MapBasedTabularView mapBasedView = MapBasedTabularView.load(output);

		Assertions.assertThat(mapBasedView.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(k1PlusK2AsExpr.getName(), 0L + 123 + 345))
				.hasSize(1);
	}
}
