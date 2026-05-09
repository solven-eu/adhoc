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
package eu.solven.adhoc.cube;

import java.util.Collection;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.measure.forest.MeasureForest;
import eu.solven.adhoc.table.InMemoryTable;
import eu.solven.adhoc.util.IHasCache;

public class TestCubeColumnsWrapper {

	@Test
	public void testGetColumns_isCached_sameInstanceAcrossCalls() {
		InMemoryTable table = InMemoryTable.builder().build();
		table.add(Map.of("c1", "v1"));

		CubeWrapper cube = CubeWrapper.builder()
				.name(this.getClass().getSimpleName())
				.forest(MeasureForest.empty())
				.table(table)
				.build();

		Collection<ColumnMetadata> first = cube.getColumns();
		Collection<ColumnMetadata> second = cube.getColumns();

		// Same Collection instance → cache is hit on second call.
		Assertions.assertThat(second).isSameAs(first);
	}

	@Test
	public void testInvalidateAll_dropsCache_andSeesUpdatedTable() {
		InMemoryTable table = InMemoryTable.builder().build();
		table.add(Map.of("c1", "v1"));

		CubeWrapper cube = CubeWrapper.builder()
				.name(this.getClass().getSimpleName())
				.forest(MeasureForest.empty())
				.table(table)
				.build();

		Collection<ColumnMetadata> before = cube.getColumns();
		Assertions.assertThat(cube.getColumnsAsMap()).containsKey("c1").doesNotContainKey("c2");

		// Add a new column to the table, then invalidate.
		table.add(Map.of("c2", 42));
		cube.invalidateAll();

		Collection<ColumnMetadata> after = cube.getColumns();
		Assertions.assertThat(after).isNotSameAs(before);
		Assertions.assertThat(cube.getColumnsAsMap()).containsKey("c1").containsKey("c2");
	}

	@Test
	public void testCubeWrapper_implementsIHasCache() {
		InMemoryTable table = InMemoryTable.builder().build();

		CubeWrapper cube = CubeWrapper.builder()
				.name(this.getClass().getSimpleName())
				.forest(MeasureForest.empty())
				.table(table)
				.build();

		// IHasCache contract → callers can erase cached state without knowing the concrete type.
		Assertions.assertThat(cube).isInstanceOf(IHasCache.class);
	}

}
