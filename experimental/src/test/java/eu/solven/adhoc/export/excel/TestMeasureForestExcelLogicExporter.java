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
package eu.solven.adhoc.export.excel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.engine.AdhocTestHelper;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.context.StandardQueryPreparator;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.eventbus.AdhocEventBusHelpersUnsafe;
import eu.solven.adhoc.eventbus.IAdhocEventBus;
import eu.solven.adhoc.factories.AdhocFactories;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.forest.UnsafeMeasureForest;
import eu.solven.adhoc.measure.sum.DivideCombination;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.Filtrator;
import eu.solven.adhoc.model.query.groupby.GroupByColumns;
import eu.solven.adhoc.table.InMemoryTable;

public class TestMeasureForestExcelLogicExporter {

	private CubeWrapper buildCube(UnsafeMeasureForest forest) {
		InMemoryTable table = InMemoryTable.builder().name("t").build();
		table.add(Map.of("a", "a1", "v1", 10L, "v2", 4L));
		table.add(Map.of("a", "a1", "v1", 30L, "v2", 6L));
		table.add(Map.of("a", "a2", "v1", 20L, "v2", 5L));

		EventBus guava = AdhocTestHelper.eventBus();
		IAdhocEventBus eventBus = AdhocEventBusHelpersUnsafe.safeWrapper(guava::post);
		AdhocFactories factories = AdhocFactories.builder().build();
		CubeQueryEngine engine = CubeQueryEngine.builder().eventBus(eventBus).factories(factories).build();

		return CubeWrapper.builder()
				.table(table)
				.engine(engine)
				.forest(forest)
				.eventBus(eventBus)
				.queryPreparator(StandardQueryPreparator.builder().build())
				.build();
	}

	/**
	 * Round-trip: build a small forest with a SUM-of-SUMs at the root, export to bytes, reopen with POI, evaluate the
	 * formula in the root column, assert it matches the engine's value.
	 */
	@Test
	public void testRoundTrip_sumOfSums() throws Exception {
		Aggregator v1Sum = Aggregator.sum("v1").toBuilder().name("v1_sum").build();
		Aggregator v2Sum = Aggregator.sum("v2").toBuilder().name("v2_sum").build();
		Combinator total = Combinator.builder()
				.name("total")
				.underlying("v1_sum")
				.underlying("v2_sum")
				.combinationKey(SumCombination.KEY)
				.build();

		UnsafeMeasureForest forest = UnsafeMeasureForest.builder().name("export_test").build();
		forest.addMeasure(v1Sum);
		forest.addMeasure(v2Sum);
		forest.addMeasure(total);
		CubeWrapper cube = buildCube(forest);

		CubeQuery query = CubeQuery.builder().measure("total").groupBy(GroupByColumns.named("a")).build();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		new MeasureForestExcelLogicExporter().export(cube, query, baos);

		try (Workbook wb = new XSSFWorkbook(new ByteArrayInputStream(baos.toByteArray()))) {
			Sheet sheet = wb.getSheet("Logic");
			Assertions.assertThat(sheet).isNotNull();

			// Header: "a", "v1_sum", "v2_sum", "total"
			Row header = sheet.getRow(0);
			Assertions.assertThat(header.getCell(0).getStringCellValue()).isEqualTo("a");
			Assertions.assertThat(header.getCell(1).getStringCellValue()).isEqualTo("v1_sum");
			Assertions.assertThat(header.getCell(2).getStringCellValue()).isEqualTo("v2_sum");
			Assertions.assertThat(header.getCell(3).getStringCellValue()).isEqualTo("total");

			// Two data rows (a=a1, a=a2). Verify the formula in the `total` column at the first data row.
			Row row1 = sheet.getRow(1);
			Assertions.assertThat(row1.getCell(3).getCellFormula()).isEqualTo("B2+C2");

			// Evaluate every formula and compare against the engine's computation (which we know: a1 → v1=40, v2=10,
			// total=50; a2 → v1=20, v2=5, total=25).
			FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();
			evaluator.evaluateAll();

			Map<String, double[]> expected =
					Map.of("a1", new double[] { 40, 10, 50 }, "a2", new double[] { 20, 5, 25 });
			for (int r = 1; r <= 2; r++) {
				Row row = sheet.getRow(r);
				String sliceValue = row.getCell(0).getStringCellValue();
				double[] exp = expected.get(sliceValue);
				Assertions.assertThat(row.getCell(1).getNumericCellValue()).isEqualTo(exp[0]);
				Assertions.assertThat(row.getCell(2).getNumericCellValue()).isEqualTo(exp[1]);
				Assertions.assertThat(row.getCell(3).getNumericCellValue()).isEqualTo(exp[2]);
			}
		}
	}

	/** Divide combinator: ensures the formula is "{left}/{right}" and Excel computes the same ratio. */
	@Test
	public void testRoundTrip_divide() throws Exception {
		Aggregator v1Sum = Aggregator.sum("v1").toBuilder().name("v1_sum").build();
		Aggregator v2Sum = Aggregator.sum("v2").toBuilder().name("v2_sum").build();
		Combinator ratio = Combinator.builder()
				.name("ratio")
				.underlying("v1_sum")
				.underlying("v2_sum")
				.combinationKey(DivideCombination.KEY)
				.build();

		UnsafeMeasureForest forest = UnsafeMeasureForest.builder().name("divide_test").build();
		forest.addMeasure(v1Sum);
		forest.addMeasure(v2Sum);
		forest.addMeasure(ratio);
		CubeWrapper cube = buildCube(forest);

		CubeQuery query = CubeQuery.builder().measure("ratio").groupBy(GroupByColumns.named("a")).build();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		new MeasureForestExcelLogicExporter().export(cube, query, baos);

		try (Workbook wb = new XSSFWorkbook(new ByteArrayInputStream(baos.toByteArray()))) {
			Sheet sheet = wb.getSheet("Logic");
			Row firstDataRow = sheet.getRow(1);
			Assertions.assertThat(firstDataRow.getCell(3).getCellFormula()).isEqualTo("B2/C2");

			FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();
			evaluator.evaluateAll();
			// a1: v1=40, v2=10, ratio=4.0 ; a2: v1=20, v2=5, ratio=4.0
			Assertions.assertThat(sheet.getRow(1).getCell(3).getNumericCellValue()).isEqualTo(4.0);
			Assertions.assertThat(sheet.getRow(2).getCell(3).getNumericCellValue()).isEqualTo(4.0);
		}
	}

	/**
	 * Filtrator whose filter column is in the query's groupBy: cell formula {@code IF(A2="a1", B2, 0)} branches on the
	 * slice value, so Excel reproduces the engine's per-slice filtered value, and editing the underlying recomputes
	 * downstream.
	 */
	@Test
	public void testRoundTrip_filtrator_inGroupBy() throws Exception {
		Aggregator v1Sum = Aggregator.sum("v1").toBuilder().name("v1_sum").build();
		Filtrator filtered = Filtrator.builder()
				.name("v1_a1")
				.underlying("v1_sum")
				.filter(eu.solven.adhoc.filter.ColumnFilter.matchEq("a", "a1"))
				.build();

		UnsafeMeasureForest forest = UnsafeMeasureForest.builder().name("filtrator_in_groupby").build();
		forest.addMeasure(v1Sum);
		forest.addMeasure(filtered);
		CubeWrapper cube = buildCube(forest);

		CubeQuery query = CubeQuery.builder().measure("v1_a1").groupBy(GroupByColumns.named("a")).build();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		new MeasureForestExcelLogicExporter().export(cube, query, baos);

		try (Workbook wb = new XSSFWorkbook(new ByteArrayInputStream(baos.toByteArray()))) {
			Sheet sheet = wb.getSheet("Logic");
			// Columns: a (0), v1_sum (1), v1_a1 (2)
			Row row1 = sheet.getRow(1);
			Assertions.assertThat(row1.getCell(2).getCellFormula()).isEqualTo("IF(A2=\"a1\",B2,0)");

			FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();
			evaluator.evaluateAll();
			// a1: v1=40, filtered=40 ; a2: v1=20, filtered=0
			Map<String, double[]> expected = Map.of("a1", new double[] { 40, 40 }, "a2", new double[] { 20, 0 });
			for (int r = 1; r <= 2; r++) {
				Row row = sheet.getRow(r);
				String sliceValue = row.getCell(0).getStringCellValue();
				double[] exp = expected.get(sliceValue);
				Assertions.assertThat(row.getCell(1).getNumericCellValue()).isEqualTo(exp[0]);
				Assertions.assertThat(row.getCell(2).getNumericCellValue()).isEqualTo(exp[1]);
			}
		}
	}

	/**
	 * Filtrator whose filter column is NOT in the query's groupBy: the engine already filtered at the table layer, so
	 * the cell carries the engine-computed inline value instead of a formula (no visible column to branch on). The
	 * workbook is still numerically correct.
	 */
	@Test
	public void testRoundTrip_filtrator_notInGroupBy() throws Exception {
		Aggregator v1Sum = Aggregator.sum("v1").toBuilder().name("v1_sum").build();
		Filtrator filtered = Filtrator.builder()
				.name("v1_a1")
				.underlying("v1_sum")
				.filter(eu.solven.adhoc.filter.ColumnFilter.matchEq("a", "a1"))
				.build();

		UnsafeMeasureForest forest = UnsafeMeasureForest.builder().name("filtrator_not_in_groupby").build();
		forest.addMeasure(v1Sum);
		forest.addMeasure(filtered);
		CubeWrapper cube = buildCube(forest);

		// No groupBy on "a" — Filtrator's filter column is invisible at the slice row.
		CubeQuery query = CubeQuery.builder().measure("v1_a1").build();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		new MeasureForestExcelLogicExporter().export(cube, query, baos);

		try (Workbook wb = new XSSFWorkbook(new ByteArrayInputStream(baos.toByteArray()))) {
			Sheet sheet = wb.getSheet("Logic");
			// One grand-total row at index 1. Columns: v1_sum (0), v1_a1 (1).
			Row row = sheet.getRow(1);
			// No formula on the Filtrator cell — inline numeric value (engine-computed = 40 for a=a1).
			Assertions.assertThat(row.getCell(1).getCellType()).isEqualTo(org.apache.poi.ss.usermodel.CellType.NUMERIC);
			Assertions.assertThat(row.getCell(1).getNumericCellValue()).isEqualTo(40.0);
		}
	}

	/** Coalesce single-underlying degenerates to just the reference (no IFERROR wrapper needed). */
	@Test
	public void testRoundTrip_coalesceSingle() throws Exception {
		Aggregator v1Sum = Aggregator.sum("v1").toBuilder().name("v1_sum").build();
		Combinator passthrough =
				Combinator.builder().name("pt").underlying("v1_sum").combinationKey(CoalesceCombination.KEY).build();

		UnsafeMeasureForest forest = UnsafeMeasureForest.builder().name("coalesce_test").build();
		forest.addMeasure(v1Sum);
		forest.addMeasure(passthrough);
		CubeWrapper cube = buildCube(forest);

		CubeQuery query = CubeQuery.builder().measure("pt").groupBy(GroupByColumns.named("a")).build();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		new MeasureForestExcelLogicExporter().export(cube, query, baos);

		try (Workbook wb = new XSSFWorkbook(new ByteArrayInputStream(baos.toByteArray()))) {
			Row row = wb.getSheet("Logic").getRow(1);
			Assertions.assertThat(row.getCell(2).getCellFormula()).isEqualTo("B2");
		}
	}
}
