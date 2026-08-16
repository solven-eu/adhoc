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

import eu.solven.adhoc.export.excel.IMeasureExcelFormula;
import eu.solven.adhoc.export.excel.RowContext;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.model.measure.IMeasure;

/**
 * Translator for {@link CoalesceCombination} — returns the first numeric underlying. Maps to nested
 * {@code IFERROR(A2, IFERROR(B2, …))} in Excel. The single-underlying case degenerates to just the cell reference; an
 * empty underlyings list is invalid (rejected upstream).
 *
 * @author Benoit Lacelle
 */
public class CombinatorCoalesceFormula implements IMeasureExcelFormula {

	@Override
	public boolean supports(IMeasure measure) {
		return measure instanceof Combinator combinator
				&& CoalesceCombination.KEY.equals(combinator.getCombinationKey());
	}

	@Override
	public Optional<String> translate(IMeasure measure, RowContext ctx) {
		List<String> underlyingCellRefs = ctx.getUnderlyingCellRefs();
		if (underlyingCellRefs.isEmpty()) {
			throw new IllegalArgumentException(
					"Combinator[COALESCE] " + measure + " has no underlyings; cannot translate to Excel formula.");
		}
		if (underlyingCellRefs.size() == 1) {
			return Optional.of(underlyingCellRefs.get(0));
		}
		// Right-fold: IFERROR(a, IFERROR(b, IFERROR(c, last))).
		StringBuilder sb = new StringBuilder();
		int n = underlyingCellRefs.size();
		for (int i = 0; i < n - 1; i++) {
			sb.append("IFERROR(").append(underlyingCellRefs.get(i)).append(',');
		}
		sb.append(underlyingCellRefs.get(n - 1)).append(")".repeat(n - 1));
		return Optional.of(sb.toString());
	}
}
