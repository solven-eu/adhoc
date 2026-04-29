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
package eu.solven.adhoc.beta.schema;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.engine.tabular.optimizer.CubeWrapperEditor;
import eu.solven.adhoc.measure.forest.MeasureForest;
import eu.solven.adhoc.table.InMemoryTable;
import eu.solven.adhoc.table.transcoder.MapTableAliaser;

public class TestCubeWrapper {
	@Test
	public void testAliasedColumns() {
		InMemoryTable table = InMemoryTable.builder().build();

		table.add(Map.of("rawC", "someV"));

		ICubeWrapper rawCube = CubeWrapper.builder()
				.name(this.getClass().getSimpleName())
				.forest(MeasureForest.empty())
				.table(table)
				.build();

		// without aliaser
		{
			ICubeWrapper cube = rawCube;

			Assertions.assertThat(cube.getColumnsAsMap())
					.containsEntry("rawC", ColumnMetadata.builder().name("rawC").type(String.class).build());
		}

		// with aliaser
		{
			ICubeWrapper cube = CubeWrapperEditor.edit(rawCube)
					.aliaser(MapTableAliaser.builder().aliasToOriginal("aliasC", "rawC").build())
					.build();

			Assertions.assertThat(cube.getColumnsAsMap())
					.containsEntry("rawC",
							ColumnMetadata.builder().name("rawC").type(String.class).alias("aliasC").build())
					// An alias column has the columnName as its own alias
					.containsEntry("aliasC",
							ColumnMetadata.builder()
									.name("aliasC")
									.type(String.class)
									.alias("rawC")
									.alias("aliasC")
									.build())
					.hasSize(2);
		}
	}

	// Add an alias to a column which is unknown by the table
	@Test
	public void testAliasedColumns_unknownUnderlying() {
		InMemoryTable table = InMemoryTable.builder().build();

		table.add(Map.of("rawC", "someV"));

		ICubeWrapper rawCube = CubeWrapper.builder()
				.name(this.getClass().getSimpleName())
				.forest(MeasureForest.empty())
				.table(table)
				.build();

		// without aliaser
		{
			ICubeWrapper cube = rawCube;

			Assertions.assertThat(cube.getColumnsAsMap())
					.containsEntry("rawC", ColumnMetadata.builder().name("rawC").type(String.class).build());
		}

		// with aliaser: the alias points to no underlying column, so it must be discarded (not registered as
		// Object.class). A shared ColumnsManager may define aliases relevant only for a subset of cubes.
		{
			ICubeWrapper cube = CubeWrapperEditor.edit(rawCube)
					.aliaser(MapTableAliaser.builder().aliasToOriginal("aliasC", "unknownC").build())
					.build();

			Assertions.assertThat(cube.getColumnsAsMap())
					.containsEntry("rawC", ColumnMetadata.builder().name("rawC").type(String.class).build())
					.hasSize(1)
					.doesNotContainKey("aliasC");
		}
	}

	// A shared ColumnsManager may carry aliases relevant only for a subset of cubes. Aliases whose underlying
	// column does not exist on this cube must be silently discarded (with a WARN log), not registered as
	// dangling Object.class columns, so they do not pollute the schema reported by `getColumns`.
	@Test
	public void testAliasedColumns_mixOfResolvedAndUnresolved() {
		InMemoryTable table = InMemoryTable.builder().build();

		table.add(Map.of("rawC", "someV"));

		ICubeWrapper rawCube = CubeWrapper.builder()
				.name(this.getClass().getSimpleName())
				.forest(MeasureForest.empty())
				.table(table)
				.build();

		ICubeWrapper cube = CubeWrapperEditor.edit(rawCube)
				.aliaser(MapTableAliaser.builder()
						// Relevant to this cube: `rawC` exists in the table.
						.aliasToOriginal("aliasC", "rawC")
						// Irrelevant to this cube: `unknownC` does not exist, and `aliasForUnknown` itself does not
						// exist. It should be discarded.
						.aliasToOriginal("aliasForUnknown", "unknownC")
						.build())
				.build();

		Assertions.assertThat(cube.getColumnsAsMap())
				.containsEntry("rawC", ColumnMetadata.builder().name("rawC").type(String.class).alias("aliasC").build())
				.containsEntry("aliasC",
						ColumnMetadata.builder()
								.name("aliasC")
								.type(String.class)
								.alias("rawC")
								.alias("aliasC")
								.build())
				// `aliasForUnknown` is absent: its underlying column does not exist on this cube.
				.doesNotContainKey("aliasForUnknown")
				.doesNotContainKey("unknownC")
				.hasSize(2);
	}

	@Test
	public void testAliasedColumns_withDot() {
		InMemoryTable table = InMemoryTable.builder().build();

		table.add(Map.of("rawC", "someV"));

		ICubeWrapper rawCube = CubeWrapper.builder()
				.name(this.getClass().getSimpleName())
				.forest(MeasureForest.empty())
				.table(table)
				.build();

		ICubeWrapper cube = CubeWrapperEditor.edit(rawCube)
				.aliaser(MapTableAliaser.builder()
						// A renaming alias
						.aliasToOriginal("aliasC", "join.rawC")
						// a not-renaming alias (like to SQL returning `rawC` as columnName for `join.rawC`)
						.aliasToOriginal("rawC", "join.rawC")
						.build())
				.build();

		// With the unqualified-fallback in place: `aliasC -> join.rawC` is resolved by stripping the
		// `join.` qualifier and finding the bare `rawC` in the table, so `aliasC` is no longer dangling.
		Assertions.assertThat(cube.getColumnsAsMap()).containsKey("rawC").containsKey("aliasC").hasSize(2);
		Assertions.assertThat(cube.getColumnsAsMap().get("aliasC").getType()).isEqualTo(String.class);
	}

	// Reproducer for the JooqTableSupplierBuilder scenario: an aliaser declares
	// aliasedColor -> b.color
	// but the table only knows the unqualified `color` (SQL backends typically return the column name without
	// the table-qualifier when listing columns — see JooqTableWrapper.getColumns). The two existing resolution
	// paths in CubeWrapper.getColumns — (1) lookup by underlying name `b.color`, (2) lookup by alias name
	// `aliasedColor` — both miss because the table's column key is the unqualified `color`. A third-try fallback
	// extracts the column part after the LAST dot from the underlying name and retries with that.
	@Test
	public void testAliasedColumns_qualifiedUnderlying_unqualifiedTableColumn() {
		InMemoryTable table = InMemoryTable.builder().build();

		// SQL-shape: the table reports the unqualified `color`, even though the join produces `b.color`.
		table.add(Map.of("color", "red"));

		ICubeWrapper rawCube = CubeWrapper.builder()
				.name(this.getClass().getSimpleName())
				.forest(MeasureForest.empty())
				.table(table)
				.build();

		ICubeWrapper cube = CubeWrapperEditor.edit(rawCube)
				.aliaser(MapTableAliaser.builder().aliasToOriginal("aliasedColor", "b.color").build())
				.build();

		// With the unqualified-fallback in place: `aliasedColor` resolves to the unqualified `color` (extracted
		// from `b.color` by stripping the table-qualifier), so the schema exposes both the original column and
		// its alias.
		Assertions.assertThat(cube.getColumnsAsMap())
				.containsEntry("color",
						ColumnMetadata.builder().name("color").type(String.class).alias("aliasedColor").build())
				.containsEntry("aliasedColor",
						ColumnMetadata.builder()
								.name("aliasedColor")
								.type(String.class)
								.alias("color")
								.alias("aliasedColor")
								.build())
				.hasSize(2);
	}

	@Test
	public void testgetHealth() {
		InMemoryTable table = InMemoryTable.builder().build();

		table.add(Map.of("rawC", "someV"));

		CubeWrapper cube = CubeWrapper.builder()
				.name(this.getClass().getSimpleName())
				.forest(MeasureForest.empty())
				.table(table)
				.build();

		Assertions.assertThat((Map) cube.getHealthDetails())
				.containsEntry("table",
						ImmutableMap.builder()
								.put("name", "inMemory")
								.put("rows", 1)
								.put("type", InMemoryTable.class.getName())
								.build())
				.hasSize(1);
	}

	@Test
	public void testMakeDetails() {
		InMemoryTable table = InMemoryTable.builder().build();
		table.add(Map.of("k", "v"));

		CubeWrapper cube = CubeWrapper.builder()
				.name(this.getClass().getSimpleName())
				.forest(MeasureForest.empty())
				.table(table)
				.build();

		Map<String, Object> details = CubeWrapper.makeDetails(cube);

		Assertions.assertThat(details).containsKey("columns").containsKey("measures");
	}

	@Test
	public void testExecuteAsync_blockingQuery_throws() {
		InMemoryTable table = InMemoryTable.builder().build();

		CubeWrapper cube = CubeWrapper.builder()
				.name(this.getClass().getSimpleName())
				.forest(MeasureForest.empty())
				.table(table)
				.build();

		eu.solven.adhoc.query.cube.CubeQuery blockingQuery = eu.solven.adhoc.query.cube.CubeQuery.builder()
				.option(eu.solven.adhoc.options.StandardQueryOptions.BLOCKING)
				.build();

		Assertions.assertThatThrownBy(() -> cube.executeAsync(blockingQuery))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("BLOCKING");
	}
}
