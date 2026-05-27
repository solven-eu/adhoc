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
package eu.solven.adhoc.pivotable.webnone.api;

/**
 * Constants related to routes offered by Pivotable.
 * 
 * @author Benoit Lacelle
 */
public interface IPivotableRouteConstants {
	String R_CUBE_QUERY = "/cubes/query";

	/**
	 * Plan-introspection routes. The {@code {queryUuid}} path variable is the {@link java.util.UUID} portion of an
	 * {@code AdhocQueryId} ({@code AdhocQueryId.queryId}). {@code /summary} returns a lightweight status object
	 * suitable for high-frequency polling; {@code /snapshot} returns the full plan tree.
	 */
	String R_CUBE_PLAN_SUMMARY = "/cubes/queries/{queryUuid}/plan/summary";
	String R_CUBE_PLAN_SNAPSHOT = "/cubes/queries/{queryUuid}/plan/snapshot";

	/**
	 * Composite-cube children: lists the registered sub-query plans whose parent equals {@code queryUuid}. Used by the
	 * UI to navigate a composite query's fan-out (root + N sub-cubes, each with its own plan). Returns a list of
	 * {@code QueryPlanSummary} — the lightweight shape suited for polling.
	 */
	String R_CUBE_PLAN_CHILDREN = "/cubes/queries/{queryUuid}/plan/children";

	/**
	 * Excel-export endpoint — returns an {@code .xlsx} workbook whose cells mirror the cube's measure logic as Excel
	 * formulas (leaf aggregators carry the engine-computed numeric value; intermediate combinators carry a formula
	 * referencing other cells). Wired only when the {@code adhoc-experimental} jar (which carries
	 * {@code MeasureForestExcelLogicExporter} and Apache POI) is on the classpath.
	 */
	String R_CUBE_EXPORT_EXCEL = "/cubes/export/excel";
}
