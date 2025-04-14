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
package eu.solven.adhoc.table;

import java.time.LocalDate;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.column.AdhocColumnsManager;
import eu.solven.adhoc.column.CalculatedColumn;
import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.table.transcoder.value.DefaultCustomTypeManager;

/**
 * Test type transcoding by converting from a Date as String into a LocalDate
 * 
 * @author Benoit Lacelle
 */
public class TestInMemoryCube_TranscodedType extends ADagTest {

	@BeforeEach
	@Override
	public void feedTable() {
		LocalDate now = LocalDate.of(2025, 4, 14);

		table.add(Map.of("c", "today", "date_as_string", now.toString(), "k", 123));
		table.add(Map.of("c", "yesterday", "date_as_string", now.minusDays(1).toString(), "k", 234));
		table.add(Map.of("c", "month_ago", "date_as_string", now.minusMonths(1).toString(), "k", 345));
		table.add(Map.of("c", "year_ago", "date_as_string", now.minusYears(1).toString(), "k", 456));

		forest.addMeasure(Aggregator.builder().name("k1").columnName("k1").aggregationKey(SumAggregation.KEY).build());
	}

	@Test
	public void testColumns_withTranscoding() {
		AdhocColumnsManager originalColumnsManager = (AdhocColumnsManager) cube.getColumnsManager();
		CubeWrapper cubeWithTranscoding = cube.toBuilder()
				.columnsManager(originalColumnsManager.toBuilder().customTypeManager(new DefaultCustomTypeManager() {
					@Override
					public Object fromTable(String column, Object coordinate) {
						if (coordinate == null) {
							return null;
						} else if ("date_as_string".equals(column)) {
							return LocalDate.parse(coordinate.toString());
						} else {
							return super.fromTable(column, coordinate);
						}
					}
				}).build())
				.build();

		Assertions.assertThat(cube.getColumns()).containsEntry("date_as_string", String.class);
		// TODO This should be reported as a LocalDate
		// Though, how can we infer the type given no data?
		Assertions.assertThat(cubeWithTranscoding.getColumns()).containsEntry("date_as_string", String.class);
	}

	@Test
	public void testColumns_withCalculatedColumn() {
		AdhocColumnsManager originalColumnsManager = (AdhocColumnsManager) cube.getColumnsManager();
		CubeWrapper cubeWithCalculated = cube.toBuilder()
				.columnsManager(originalColumnsManager.toBuilder()
						.calculatedColumn(CalculatedColumn.builder()
								.name("date")
								.type(LocalDate.class)
								.recordToCoordinate(record -> {
									Object dateAsString = record.getGroupBy("date_as_string");
									if (dateAsString == null) {
										return null;
									} else {
										return LocalDate.parse(dateAsString.toString());
									}
								})
								.build())
						.build())
				.build();

		Assertions.assertThat(cube.getColumns()).containsEntry("date_as_string", String.class);
		Assertions.assertThat(cubeWithCalculated.getColumns())
				.containsEntry("date_as_string", String.class)
				.containsEntry("date", LocalDate.class);
	}

}
