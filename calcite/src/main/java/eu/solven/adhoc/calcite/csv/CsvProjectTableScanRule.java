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
package eu.solven.adhoc.calcite.csv;

import java.util.List;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Planner rule that projects from a {@link CsvTableScan} scan just the columns needed to satisfy a projection. If the
 * projection's expressions are trivial, the projection is removed.
 *
 * @see CsvRules#PROJECT_SCAN
 */
// @Value.Enclosing
public class CsvProjectTableScanRule extends RelRule<CsvProjectTableScanRule.Config> {

	/** Creates a CsvProjectTableScanRule. */
	protected CsvProjectTableScanRule(Config config) {
		super(config);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		final LogicalProject project = call.rel(0);
		final CsvTableScan scan = call.rel(1);
		@Nullable
		int[] fields = getProjectFields(project.getProjects());
		if (fields == null) {
			// Project contains expressions more complex than just field references.
			return;
		}
		call.transformTo(new CsvTableScan(scan.getCluster(), scan.getTable(), scan.csvTable, fields));
	}

	private static int @Nullable [] getProjectFields(List<RexNode> exps) {
		final int[] fields = new int[exps.size()];
		for (int i = 0; i < exps.size(); i++) {
			final RexNode exp = exps.get(i);
			if (exp instanceof RexInputRef) {
				fields[i] = ((RexInputRef) exp).getIndex();
			} else {
				return null; // not a simple projection
			}
		}
		return fields;
	}

	/** Rule configuration. */
	// @Value.Immutable(singleton = false)
	public interface Config extends RelRule.Config {
		// Config DEFAULT = ImmutableCsvProjectTableScanRule.Config.builder()
		// .withOperandSupplier(b0 -> b0.operand(LogicalProject.class)
		// .oneInput(b1 -> b1.operand(CsvTableScan.class).noInputs()))
		// .build();

		@Override
		default CsvProjectTableScanRule toRule() {
			return new CsvProjectTableScanRule(this);
		}
	}
}
