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
package eu.solven.adhoc.filter.logicng;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.logicng.formulas.Formula;
import org.logicng.formulas.FormulaFactory;
import org.logicng.formulas.Variable;
import org.logicng.transformations.dnf.DNFFactorization;

public class LogicNGExperiment {
	FormulaFactory f = new FormulaFactory();
	Variable c1 = f.variable("c1");
	Variable d1 = f.variable("d1");
	Formula c1Ord1 = f.or(c1, d1);

	Variable d2 = f.variable("d2");
	Formula c1Ord2 = f.or(c1, d2);

	Variable e1 = f.variable("e1");
	Formula c1Ore1 = f.or(c1, e1);

	Variable f1 = f.variable("f1");
	Formula f1Ord1 = f.or(f1, d1);

	Formula formula = f.and(c1Ord1, c1Ord2, c1Ore1, f1Ord1);

	@Test
	void dnf() {
		Assertions.assertThat(formula).hasToString("(c1 | d1) & (c1 | d2) & (c1 | e1) & (f1 | d1)");

		Formula dnf = formula.transform(new DNFFactorization());
		Assertions.assertThat(dnf)
				.hasToString(
						"f1 & c1 | c1 & d1 | f1 & e1 & c1 | e1 & c1 & d1 | f1 & d2 & c1 | c1 & d2 & d1 | f1 & e1 & d2 & c1 | d1 & e1 & d2 & c1 | f1 & c1 & d1 | f1 & e1 & c1 & d1 | f1 & c1 & d2 & d1 | f1 & e1 & d2 & d1 | e1 & d2 & d1");
	}
}
