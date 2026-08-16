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

import java.util.Optional;

import eu.solven.adhoc.model.measure.IMeasure;

/**
 * Translates a single non-leaf {@link IMeasure} into the body of an Excel formula. Implementations exist per
 * {@code (measure-class, combination-key)} pair; the translator registry walks its implementations until one
 * {@link #supports(IMeasure)} the measure under consideration.
 *
 * @author Benoit Lacelle
 */
public interface IMeasureExcelFormula {

	/**
	 * @return true iff this translator knows how to express {@code measure} as an Excel formula. Implementations
	 *         typically check the measure's class and combination key.
	 */
	boolean supports(IMeasure measure);

	/**
	 * @param measure
	 *            the measure to translate
	 * @param ctx
	 *            per-row context exposing underlying-cell refs and groupBy-cell refs
	 * @return the formula body (without the leading {@code =}), e.g. {@code "B2+C2"}; or {@link Optional#empty()} to
	 *         signal "no formula representable from the visible cells, fall back to the engine-computed inline value".
	 *         The empty case is used by {@code FiltratorFormula} when the filter references columns not in the query's
	 *         groupBy — the filter has been applied at the table level so the engine-computed value already reflects
	 *         it, but there is no cell-level formula that reproduces the dependency.
	 */
	Optional<String> translate(IMeasure measure, RowContext ctx);
}
