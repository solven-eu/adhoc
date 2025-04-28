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

import java.time.LocalDate;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.beta.schema.IAdhocSchema.AdhocSchemaQuery;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.data.tabular.ITabularView;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.table.InMemoryTable;

public class TestAdhocSchema {
	@Test
	public void testColumns() {
		AdhocSchema schema = AdhocSchema.builder().build();

		InMemoryTable table = InMemoryTable.builder().name("simpleTable").build();
		schema.registerTable(table);

		table.add(Map.of("k", "v"));

		EndpointSchemaMetadata schemaAll = schema.getMetadata(AdhocSchemaQuery.builder().build(), true);
		Assertions.assertThat(schemaAll.getTables()).containsKey("simpleTable");
	}

	@Test
	public void testColumns_notAll() {
		AdhocSchema schema = AdhocSchema.builder().build();

		InMemoryTable table = InMemoryTable.builder().name("simpleTable").build();
		schema.registerTable(table);

		table.add(Map.of("k", "v"));

		EndpointSchemaMetadata schemaAll = schema.getMetadata(AdhocSchemaQuery.builder().build(), false);
		Assertions.assertThat(schemaAll.getTables()).isEmpty();
	}

	@Test
	public void testTranscodeFilter() {
		InMemoryTable table = InMemoryTable.builder().name("simple").build();

		LocalDate today = LocalDate.now();
		table.add(Map.of("k", "v", "date", today));

		AdhocSchema schema = AdhocSchema.builder().build();
		schema.registerTable(table);
		schema.registerForest(MeasureForest.builder().name("simple").measure(Aggregator.countAsterisk()).build());

		CubeWrapper cube = schema.registerCube("simple", "simple", "simple");

		Assertions.assertThat(cube.getColumnTypes()).containsEntry("date", LocalDate.class);

		ITabularView view = schema.execute("simple",
				AdhocQuery.builder()
						.measure(Aggregator.countAsterisk().getName())
						.andFilter("date", today.toString())
						.build());
		MapBasedTabularView mapBasedView = MapBasedTabularView.load(view);
		Assertions.assertThat(mapBasedView.getCoordinatesToValues())
				.containsEntry(Map.of(), Map.of(Aggregator.countAsterisk().getName(), 0L + 1))
				.hasSize(1);
	}
}
