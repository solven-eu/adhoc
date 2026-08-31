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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.cube.ICubeWrapper;
import eu.solven.adhoc.dataframe.tabular.ITabularView;
import eu.solven.adhoc.dataframe.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.query.CubeQuery;
import eu.solven.adhoc.model.measure.Aggregator;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.Filtrator;
import eu.solven.adhoc.model.measure.IMeasure;

/**
 * Exports a measure forest as an Excel workbook whose cells mirror the measure logic. Each leaf {@link Aggregator}
 * column carries the engine-computed numeric value (inline); each {@link Combinator} column carries an Excel formula
 * referencing its underlying columns at the same row. Opening the workbook reproduces the engine's output and lets a
 * consumer modify leaf values to see the downstream impact recompute Excel-natively.
 *
 * <p>
 * v1 limitations (deliberate, enforced by validation):
 * <ul>
 * <li>Only {@link Aggregator} (leaf) and {@link Combinator} measures are supported. {@code Filtrator},
 * {@code Unfiltrator}, {@code Partitionor}, {@code Shiftor}, custom {@code IMeasure}s — refused with a clear
 * error.</li>
 * <li>Single sheet at the query's groupBy granularity. Cross-granularity steps (Partitionor / Unfiltrator) require a
 * multi-sheet layout that v2 will add.</li>
 * <li>Built-in Combinator combination keys only (SUM, PRODUCT, DIVIDE, SUBTRACT, MAX, MIN, COALESCE). Custom
 * combinations register their translator via {@link ExcelExportConfig#getFormulaTranslator()}.</li>
 * <li>Query custom marker and options are passed verbatim to the engine but not surfaced in the workbook.</li>
 * </ul>
 *
 * @author Benoit Lacelle
 */
public class MeasureForestExcelLogicExporter {

	private final ExcelExportConfig config;

	public MeasureForestExcelLogicExporter() {
		this(ExcelExportConfig.builder().build());
	}

	public MeasureForestExcelLogicExporter(ExcelExportConfig config) {
		this.config = config;
	}

	/** Writes the {@code .xlsx} workbook to {@code out}. The stream is left open for the caller to close. */
	public void export(ICubeWrapper cube, CubeQuery query, OutputStream out) throws IOException {
		Map<String, IMeasure> nameToMeasure = cube.getNameToMeasure();

		// Resolve query roots through the forest, then walk down each one collecting every reachable measure in
		// post-order (children before parents). Memoised via `visited` so a shared underlying lands once.
		List<IMeasure> postOrder = collectPostOrder(query, nameToMeasure);

		// Validate every reachable measure is translatable. Fails fast with a clear error naming the offender so
		// the consumer knows exactly which measure made the export bail.
		validate(postOrder);

		// Execute the query with EVERY reachable measure listed as a root so the resulting view carries all
		// intermediate values. The original measure set is replaced — filter/groupBy/customMarker survive.
		CubeQuery expandedQuery = expandQuery(query, postOrder);
		ITabularView view = cube.execute(expandedQuery);
		Map<Map<String, ?>, Map<String, ?>> slicesToValues = MapBasedTabularView.load(view).getCoordinatesToValues();

		writeWorkbook(query, postOrder, slicesToValues, out);
	}

	protected List<IMeasure> collectPostOrder(CubeQuery query, Map<String, IMeasure> nameToMeasure) {
		List<IMeasure> result = new ArrayList<>();
		LinkedHashSet<String> visited = new LinkedHashSet<>();
		for (IMeasure root : query.getMeasures()) {
			IMeasure resolved = nameToMeasure.get(root.getName());
			if (resolved == null) {
				throw new IllegalArgumentException(
						"Queried measure '" + root.getName() + "' is not registered in the cube's forest");
			}
			walkPostOrder(resolved, nameToMeasure, visited, result);
		}
		return result;
	}

	private void walkPostOrder(IMeasure measure,
			Map<String, IMeasure> nameToMeasure,
			LinkedHashSet<String> visited,
			List<IMeasure> out) {
		if (!visited.add(measure.getName())) {
			return;
		}
		if (measure instanceof Combinator combinator) {
			for (String underlyingName : combinator.getUnderlyings()) {
				resolveAndWalk(measure, underlyingName, nameToMeasure, visited, out);
			}
		} else if (measure instanceof Filtrator filtrator) {
			resolveAndWalk(measure, filtrator.getUnderlying(), nameToMeasure, visited, out);
		}
		out.add(measure);
	}

	private void resolveAndWalk(IMeasure parent,
			String underlyingName,
			Map<String, IMeasure> nameToMeasure,
			Set<String> visited,
			List<IMeasure> out) {
		IMeasure child = nameToMeasure.get(underlyingName);
		if (child == null) {
			throw new IllegalArgumentException(parent.getClass().getSimpleName() + " '"
					+ parent.getName()
					+ "' references unknown underlying measure '"
					+ underlyingName
					+ "'");
		}
		walkPostOrder(child, nameToMeasure, visited, out);
	}

	protected void validate(List<IMeasure> measures) {
		for (IMeasure measure : measures) {
			if (measure instanceof Aggregator) {
				continue;
			}
			if (measure instanceof Combinator || measure instanceof Filtrator) {
				if (!config.getFormulaTranslator().supports(measure)) {
					throw new IllegalArgumentException("No Excel formula translator registered for " + measure
							+ ". Register a custom "
							+ "translator via ExcelExportConfig.builder().formulaTranslator(...).");
				}
				continue;
			}
			throw new IllegalArgumentException("Excel export supports Aggregator, Combinator, and Filtrator; got "
					+ measure.getClass().getSimpleName()
					+ " for measure '"
					+ measure.getName()
					+ "'. See "
					+ "MeasureForestExcelLogicExporter Javadoc for the v1 scope and the v2 roadmap.");
		}
	}

	protected CubeQuery expandQuery(CubeQuery original, List<IMeasure> measures) {
		List<String> names = new ArrayList<>();
		for (IMeasure m : measures) {
			names.add(m.getName());
		}
		// Rebuild from scratch — `CubeQueryBuilder.measures(Collection)` appends rather than replaces, so we can't
		// reuse `original.toBuilder()`. The post-order already contains every queried root.
		return CubeQuery.builder()
				.filter(original.getFilter())
				.groupBy(original.getGroupBy())
				.customMarker(original.getCustomMarker())
				.options(original.getOptions())
				.measureNames(names)
				.build();
	}

	protected void writeWorkbook(CubeQuery query,
			List<IMeasure> measures,
			Map<Map<String, ?>, Map<String, ?>> slicesToValues,
			OutputStream out) throws IOException {
		try (Workbook workbook = new XSSFWorkbook()) {
			Sheet sheet = workbook.createSheet(config.getSheetName());
			List<String> groupByCols = new ArrayList<>(query.getGroupBy().getSortedColumns());
			Map<String, Integer> measureNameToColumn = writeHeader(sheet, groupByCols, measures);
			writeRows(sheet, groupByCols, measures, measureNameToColumn, slicesToValues);
			workbook.write(out);
		}
	}

	private Map<String, Integer> writeHeader(Sheet sheet, List<String> groupByCols, List<IMeasure> measures) {
		Row header = sheet.createRow(0);
		int col = 0;
		for (String gbCol : groupByCols) {
			header.createCell(col).setCellValue(gbCol);
			col++;
		}
		Map<String, Integer> measureNameToColumn = new LinkedHashMap<>();
		for (IMeasure m : measures) {
			measureNameToColumn.put(m.getName(), col);
			header.createCell(col).setCellValue(m.getName());
			col++;
		}
		return measureNameToColumn;
	}

	private void writeRows(Sheet sheet,
			List<String> groupByCols,
			List<IMeasure> measures,
			Map<String, Integer> measureNameToColumn,
			Map<Map<String, ?>, Map<String, ?>> slicesToValues) {
		int rowIdx = 1;
		for (Map.Entry<Map<String, ?>, Map<String, ?>> entry : slicesToValues.entrySet()) {
			Row row = sheet.createRow(rowIdx);
			writeSliceColumns(row, groupByCols, entry.getKey());
			writeMeasureColumns(row, rowIdx, groupByCols, measures, measureNameToColumn, entry.getValue());
			rowIdx++;
		}
	}

	private void writeSliceColumns(Row row, List<String> groupByCols, Map<String, ?> slice) {
		int c = 0;
		for (String gbCol : groupByCols) {
			Object value = slice.get(gbCol);
			String text;
			if (value == null) {
				text = "";
			} else {
				text = value.toString();
			}
			row.createCell(c).setCellValue(text);
			c++;
		}
	}

	private void writeMeasureColumns(Row row,
			int rowIdx,
			List<String> groupByCols,
			List<IMeasure> measures,
			Map<String, Integer> measureNameToColumn,
			Map<String, ?> sliceValues) {
		// Cache groupBy cell refs once per row — every measure on the row may need them.
		Map<String, String> groupByCellRefs = new LinkedHashMap<>();
		for (int i = 0; i < groupByCols.size(); i++) {
			groupByCellRefs.put(groupByCols.get(i), CellReference.convertNumToColString(i) + (rowIdx + 1));
		}
		Set<String> groupByColumnSet = Set.copyOf(groupByCols);

		for (IMeasure measure : measures) {
			int col = measureNameToColumn.get(measure.getName());
			Cell cell = row.createCell(col);
			if (measure instanceof Aggregator) {
				inlineEngineValue(cell, sliceValues, measure);
				continue;
			}
			List<String> underlyingNames = underlyingNames(measure);
			List<String> refs = new ArrayList<>();
			for (String underlying : underlyingNames) {
				int underlyingCol = measureNameToColumn.get(underlying);
				// A1 notation: convertNumToColString is 0-indexed for columns; row is 1-indexed.
				refs.add(CellReference.convertNumToColString(underlyingCol) + (rowIdx + 1));
			}
			RowContext ctx = new RowContextImpl(refs, groupByCellRefs, groupByColumnSet);
			Optional<String> formula = config.getFormulaTranslator().translate(measure, ctx);
			if (formula.isPresent()) {
				cell.setCellFormula(formula.get());
			} else {
				// Translator opted out (e.g. Filtrator with filter columns outside the groupBy). Fall back to the
				// engine-computed numeric value for this cell.
				inlineEngineValue(cell, sliceValues, measure);
			}
		}
	}

	private void inlineEngineValue(Cell cell, Map<String, ?> sliceValues, IMeasure measure) {
		Object value = sliceValues.get(measure.getName());
		if (value instanceof Number numeric) {
			cell.setCellValue(numeric.doubleValue());
		}
		// else: leave the cell blank — Excel reads as empty, formulas using IFERROR/COALESCE shape handle it.
	}

	private List<String> underlyingNames(IMeasure measure) {
		if (measure instanceof Combinator combinator) {
			return List.copyOf(combinator.getUnderlyings());
		}
		if (measure instanceof Filtrator filtrator) {
			return ImmutableList.of(filtrator.getUnderlying());
		}
		throw new IllegalStateException("Unexpected non-leaf measure type: " + measure);
	}

	/** Captures the per-row cell refs the translator needs. Built once per data row. */
	private static final class RowContextImpl implements RowContext {
		private final List<String> underlyingCellRefs;
		private final Map<String, String> groupByCellRefs;
		private final Set<String> groupByColumns;

		private RowContextImpl(List<String> underlyingCellRefs,
				Map<String, String> groupByCellRefs,
				Set<String> groupByColumns) {
			this.underlyingCellRefs = underlyingCellRefs;
			this.groupByCellRefs = groupByCellRefs;
			this.groupByColumns = groupByColumns;
		}

		@Override
		public List<String> getUnderlyingCellRefs() {
			return underlyingCellRefs;
		}

		@Override
		public String getGroupByCellRef(String columnName) {
			String ref = groupByCellRefs.get(columnName);
			if (ref == null) {
				throw new IllegalArgumentException("Column '" + columnName
						+ "' is not in the query's groupBy "
						+ groupByColumns
						+ "; cannot build a cell-level Excel formula referencing it.");
			}
			return ref;
		}

		@Override
		public Set<String> getGroupByColumns() {
			return groupByColumns;
		}
	}
}
