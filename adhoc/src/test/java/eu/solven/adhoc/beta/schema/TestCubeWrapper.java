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

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.engine.tabular.optimizer.CubeWrapperEditor;
import eu.solven.adhoc.measure.MeasureForest;
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

		// with aliaser
		{
			ICubeWrapper cube = CubeWrapperEditor.edit(rawCube)
					.aliaser(MapTableAliaser.builder().aliasToOriginal("aliasC", "unknownC").build())
					.build();

			Assertions.assertThat(cube.getColumnsAsMap())
					.containsEntry("rawC", ColumnMetadata.builder().name("rawC").type(String.class).build())
					.containsEntry("aliasC",
							ColumnMetadata.builder().name("aliasC").tag("alias").type(Object.class).build())
					.hasSize(2);
		}
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

		Assertions.assertThat(cube.getColumnsAsMap())
				.containsEntry("rawC", ColumnMetadata.builder().name("rawC").type(String.class).alias("rawC").build())
				// `aliasC` is dangling with no explicit type as `tableColumn=join.rawC` did not match the unqualified
				// name `rawC` from table.
				.containsEntry("aliasC",
						ColumnMetadata.builder().name("aliasC").type(Object.class).tag("alias").build())
				.hasSize(2);
	}
}
