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
