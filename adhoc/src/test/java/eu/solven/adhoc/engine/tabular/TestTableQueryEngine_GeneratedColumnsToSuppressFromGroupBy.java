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
package eu.solven.adhoc.engine.tabular;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.column.FunctionCalculatedColumn;
import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * Unit tests for {@link TableQueryEngine#generatedColumnsToSuppressFromGroupBy(IGroupBy, Set)}.
 *
 * Focus: the regression fix that protects {@link eu.solven.adhoc.column.ICalculatedColumn}s from being wrongly
 * collapsed to the {@link eu.solven.adhoc.column.generated_column.IColumnGenerator} fallback. See
 * {@code TestTableQuery_DuckDb_VaR#testGroupByScenarioIndex_withStar_countAsterisk}.
 */
public class TestTableQueryEngine_GeneratedColumnsToSuppressFromGroupBy {

	@Test
	public void testPlainReferencedColumn_isSuppressed() {
		IGroupBy groupBy = GroupByColumns.named("scenarioIndex");

		Set<String> suppressed =
				TableQueryEngine.generatedColumnsToSuppressFromGroupBy(groupBy, ImmutableSet.of("scenarioIndex"));

		Assertions.assertThat(suppressed).containsExactly("scenarioIndex");
	}

	@Test
	public void testUnrelatedColumn_isNotSuppressed() {
		IGroupBy groupBy = GroupByColumns.named("country");

		Set<String> suppressed =
				TableQueryEngine.generatedColumnsToSuppressFromGroupBy(groupBy, ImmutableSet.of("scenarioIndex"));

		Assertions.assertThat(suppressed).isEmpty();
	}

	@Test
	public void testGrandTotal_returnsEmpty() {
		Set<String> suppressed = TableQueryEngine.generatedColumnsToSuppressFromGroupBy(IGroupBy.GRAND_TOTAL,
				ImmutableSet.of("scenarioIndex"));

		Assertions.assertThat(suppressed).isEmpty();
	}

	@Test
	public void testNoGeneratedColumns_returnsEmpty() {
		IGroupBy groupBy = GroupByColumns.named("scenarioIndex");

		Set<String> suppressed =
				TableQueryEngine.generatedColumnsToSuppressFromGroupBy(groupBy, ImmutableSet.<String>of());

		Assertions.assertThat(suppressed).isEmpty();
	}

	/**
	 * Regression: a {@link FunctionCalculatedColumn} (e.g. the `*` grandTotal coordinate) must NOT be suppressed even
	 * when its name matches a generated column, because it carries its own value-computation logic which would be lost
	 * by the IColumnGenerator fallback.
	 */
	@Test
	public void testCalculatedColumn_isKept_evenWhenNameMatchesGenerated() {
		FunctionCalculatedColumn calculated = FunctionCalculatedColumn.builder()
				.name("scenarioIndex")
				.type(String.class)
				.recordToCoordinate(record -> "*")
				.skipFiltering(true)
				.build();
		IGroupBy groupBy = GroupByColumns.of(ImmutableSet.of(calculated));

		Set<String> suppressed =
				TableQueryEngine.generatedColumnsToSuppressFromGroupBy(groupBy, ImmutableSet.of("scenarioIndex"));

		Assertions.assertThat(suppressed).isEmpty();
	}

	/**
	 * Regression: in a query mixing a plain {@link ReferencedColumn} and a {@link FunctionCalculatedColumn} both named
	 * like a generated column, only the plain reference is suppressible. NOTE: this scenario cannot be expressed by a
	 * single {@link IGroupBy} (it indexes columns by name), so we exercise the two cases separately and assert the
	 * discrimination holds.
	 */
	@Test
	public void testCalculatedColumn_preservedWhileReferencedColumn_suppressed() {
		Set<String> generatedColumns = ImmutableSet.of("scenarioIndex");

		IGroupBy referencedGroupBy = GroupByColumns.named("scenarioIndex");
		IGroupBy calculatedGroupBy = GroupByColumns.of(ImmutableSet.of(FunctionCalculatedColumn.builder()
				.name("scenarioIndex")
				.type(String.class)
				.recordToCoordinate(record -> "*")
				.build()));

		Assertions
				.assertThat(TableQueryEngine.generatedColumnsToSuppressFromGroupBy(referencedGroupBy, generatedColumns))
				.containsExactly("scenarioIndex");
		Assertions
				.assertThat(TableQueryEngine.generatedColumnsToSuppressFromGroupBy(calculatedGroupBy, generatedColumns))
				.isEmpty();
	}
}
