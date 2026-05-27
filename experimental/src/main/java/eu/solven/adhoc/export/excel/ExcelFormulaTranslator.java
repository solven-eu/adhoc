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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.export.excel.builtin.CombinatorCoalesceFormula;
import eu.solven.adhoc.export.excel.builtin.CombinatorDivideFormula;
import eu.solven.adhoc.export.excel.builtin.CombinatorMaxFormula;
import eu.solven.adhoc.export.excel.builtin.CombinatorMinFormula;
import eu.solven.adhoc.export.excel.builtin.CombinatorProductFormula;
import eu.solven.adhoc.export.excel.builtin.CombinatorSubtractFormula;
import eu.solven.adhoc.export.excel.builtin.CombinatorSumFormula;
import eu.solven.adhoc.export.excel.builtin.FiltratorFormula;
import eu.solven.adhoc.model.measure.IMeasure;

/**
 * Registry of {@link IMeasureExcelFormula}s. The first registered translator that {@link IMeasureExcelFormula#supports
 * supports} a given measure wins. {@link #defaults()} ships the seven built-in
 * {@link eu.solven.adhoc.model.measure.Combinator}-key translations (SUM, PRODUCT, DIVIDE, SUBTRACT, MAX, MIN,
 * COALESCE).
 *
 * @author Benoit Lacelle
 */
public final class ExcelFormulaTranslator {

	private final ImmutableList<IMeasureExcelFormula> translators;

	private ExcelFormulaTranslator(List<IMeasureExcelFormula> translators) {
		this.translators = ImmutableList.copyOf(translators);
	}

	/**
	 * @return the formula body (without the leading {@code =}) for {@code measure}, or {@link Optional#empty()} when
	 *         the supporting translator wants the exporter to fall back to the inline engine-computed value (see
	 *         {@link IMeasureExcelFormula#translate}).
	 * @throws IllegalArgumentException
	 *             when no registered translator supports {@code measure}.
	 */
	public Optional<String> translate(IMeasure measure, RowContext ctx) {
		for (IMeasureExcelFormula t : translators) {
			if (t.supports(measure)) {
				return t.translate(measure, ctx);
			}
		}
		throw new IllegalArgumentException("No Excel formula translator registered for measure " + measure
				+ " (class="
				+ measure.getClass().getSimpleName()
				+ "). Built-ins cover Combinator (built-in combination keys) and Filtrator.");
	}

	/** @return true iff any registered translator supports {@code measure}. */
	public boolean supports(IMeasure measure) {
		return translators.stream().anyMatch(t -> t.supports(measure));
	}

	public static Builder builder() {
		return new Builder();
	}

	/** Pre-configured translator registry with the shipped Combinator translations + the Filtrator translator. */
	public static ExcelFormulaTranslator defaults() {
		return builder().with(new CombinatorSumFormula())
				.with(new CombinatorProductFormula())
				.with(new CombinatorDivideFormula())
				.with(new CombinatorSubtractFormula())
				.with(new CombinatorMaxFormula())
				.with(new CombinatorMinFormula())
				.with(new CombinatorCoalesceFormula())
				.with(new FiltratorFormula())
				.build();
	}

	/** Mutable builder for {@link ExcelFormulaTranslator}. */
	public static final class Builder {
		private final List<IMeasureExcelFormula> translators = new ArrayList<>();

		/** Adds {@code translator} at the END of the registry — first-registered wins. */
		public Builder with(IMeasureExcelFormula translator) {
			translators.add(translator);
			return this;
		}

		public ExcelFormulaTranslator build() {
			return new ExcelFormulaTranslator(translators);
		}
	}
}
