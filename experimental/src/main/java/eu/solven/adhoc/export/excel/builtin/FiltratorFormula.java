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
package eu.solven.adhoc.export.excel.builtin;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import eu.solven.adhoc.export.excel.FilterToExcelPredicate;
import eu.solven.adhoc.export.excel.IMeasureExcelFormula;
import eu.solven.adhoc.export.excel.RowContext;
import eu.solven.adhoc.model.measure.Filtrator;
import eu.solven.adhoc.model.measure.IMeasure;

/**
 * Translator for {@link Filtrator}. Two cases based on whether the filter columns are visible at the slice row:
 *
 * <ul>
 * <li><strong>All filter columns are in the query's groupBy</strong> — emits
 * {@code IF(<predicate>, <underlyingCell>, 0)}. The {@code IF} branches on the slice's column values; for slices
 * matching the filter the Filtrator's value equals the underlying's value at that slice, otherwise 0.</li>
 * <li><strong>Any filter column is NOT in the groupBy</strong> — returns {@link Optional#empty()}. The engine applied
 * the filter at the table layer, so the Filtrator's value is already correct in the data, but there is no cell-level
 * formula that reproduces it from the visible columns. The exporter falls back to the engine-computed inline numeric
 * value for that cell. The workbook remains correct; it just doesn't link this cell to other cells via a formula.</li>
 * </ul>
 *
 * <p>
 * Filter shapes supported: {@code IColumnFilter} with {@code EqualsMatcher} or {@code InMatcher}, plus boolean
 * {@code AndFilter} / {@code OrFilter} / {@code NotFilter} compositions. Anything else throws — see
 * {@link FilterToExcelPredicate}.
 *
 * @author Benoit Lacelle
 */
public class FiltratorFormula implements IMeasureExcelFormula {

	@Override
	public boolean supports(IMeasure measure) {
		return measure instanceof Filtrator;
	}

	@Override
	public Optional<String> translate(IMeasure measure, RowContext ctx) {
		Filtrator filtrator = (Filtrator) measure;
		Set<String> filterColumns = FilterToExcelPredicate.columnsReferenced(filtrator.getFilter());
		if (!ctx.getGroupByColumns().containsAll(filterColumns)) {
			// Filter references columns outside the query's groupBy — the engine has already filtered at the table
			// layer, so the engine-computed value for this slice is already what the Filtrator represents. No cell-
			// level formula expresses the dependency on the visible underlying cell (it holds the un-filtered sum),
			// so the exporter writes the inline value instead.
			return Optional.empty();
		}
		String predicate = FilterToExcelPredicate.compile(filtrator.getFilter(), ctx);
		List<String> underlyingRefs = ctx.getUnderlyingCellRefs();
		if (underlyingRefs.size() != 1) {
			throw new IllegalArgumentException(
					"Filtrator " + measure + " should have exactly one underlying; got " + underlyingRefs.size());
		}
		return Optional.of("IF(" + predicate + "," + underlyingRefs.get(0) + ",0)");
	}
}
