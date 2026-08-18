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

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.aggregation.comparable.MaxCombination;
import eu.solven.adhoc.measure.aggregation.comparable.MinCombination;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import eu.solven.adhoc.measure.sum.DivideCombination;
import eu.solven.adhoc.measure.sum.ProductCombination;
import eu.solven.adhoc.measure.sum.SubstractionCombination;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.model.measure.Combinator;

public class TestExcelFormulaTranslator {

	private final ExcelFormulaTranslator translator = ExcelFormulaTranslator.defaults();

	private Combinator combinator(String combinationKey, String... underlyings) {
		return Combinator.builder().name("m").underlyings(List.of(underlyings)).combinationKey(combinationKey).build();
	}

	/** Minimal {@link RowContext} for combinator tests — only the underlying cell refs matter. */
	private static RowContext ctx(String... refs) {
		List<String> underlyings = List.of(refs);
		return new RowContext() {
			@Override
			public List<String> getUnderlyingCellRefs() {
				return underlyings;
			}

			@Override
			public String getGroupByCellRef(String columnName) {
				throw new UnsupportedOperationException("not used by combinator translators");
			}

			@Override
			public Set<String> getGroupByColumns() {
				return Set.of();
			}
		};
	}

	@Test
	public void testSum_twoUnderlyings() {
		Assertions.assertThat(translator.translate(combinator(SumCombination.KEY, "a", "b"), ctx("B2", "C2")))
				.contains("B2+C2");
	}

	@Test
	public void testSum_threeUnderlyings() {
		Assertions
				.assertThat(translator.translate(combinator(SumCombination.KEY, "a", "b", "c"), ctx("B2", "C2", "D2")))
				.contains("B2+C2+D2");
	}

	@Test
	public void testProduct() {
		Assertions.assertThat(translator.translate(combinator(ProductCombination.KEY, "a", "b"), ctx("B2", "C2")))
				.contains("B2*C2");
	}

	@Test
	public void testDivide() {
		Assertions.assertThat(translator.translate(combinator(DivideCombination.KEY, "a", "b"), ctx("B2", "C2")))
				.contains("B2/C2");
	}

	@Test
	public void testSubtract() {
		Assertions.assertThat(translator.translate(combinator(SubstractionCombination.KEY, "a", "b"), ctx("B2", "C2")))
				.contains("B2-C2");
	}

	@Test
	public void testMax() {
		Assertions
				.assertThat(translator.translate(combinator(MaxCombination.KEY, "a", "b", "c"), ctx("B2", "C2", "D2")))
				.contains("MAX(B2,C2,D2)");
	}

	@Test
	public void testMin() {
		Assertions.assertThat(translator.translate(combinator(MinCombination.KEY, "a", "b"), ctx("B2", "C2")))
				.contains("MIN(B2,C2)");
	}

	@Test
	public void testCoalesce_single() {
		Assertions.assertThat(translator.translate(combinator(CoalesceCombination.KEY, "a"), ctx("B2"))).contains("B2");
	}

	@Test
	public void testCoalesce_two() {
		Assertions.assertThat(translator.translate(combinator(CoalesceCombination.KEY, "a", "b"), ctx("B2", "C2")))
				.contains("IFERROR(B2,C2)");
	}

	@Test
	public void testCoalesce_three() {
		Assertions
				.assertThat(
						translator.translate(combinator(CoalesceCombination.KEY, "a", "b", "c"), ctx("B2", "C2", "D2")))
				.contains("IFERROR(B2,IFERROR(C2,D2))");
	}

	@Test
	public void testUnknownCombinationKey_throws() {
		Combinator unsupported = Combinator.builder()
				.name("m")
				.underlying("a")
				.combinationKey("eu.solven.adhoc.SOME_UNKNOWN_KEY")
				.build();
		Assertions.assertThatThrownBy(() -> translator.translate(unsupported, ctx("B2")))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("No Excel formula translator");
	}

	@Test
	public void testCustomTranslator_overridesBuiltin() {
		IMeasureExcelFormula always7 = new IMeasureExcelFormula() {
			@Override
			public boolean supports(eu.solven.adhoc.model.measure.IMeasure m) {
				return true;
			}

			@Override
			public Optional<String> translate(eu.solven.adhoc.model.measure.IMeasure m, RowContext c) {
				return Optional.of("7");
			}
		};
		ExcelFormulaTranslator custom = ExcelFormulaTranslator.builder()
				.with(always7)
				.with(new eu.solven.adhoc.export.excel.builtin.CombinatorSumFormula())
				.build();
		// First-registered wins, so the custom one should win even over a SUM combinator.
		Assertions.assertThat(custom.translate(combinator(SumCombination.KEY, "a"), ctx("B2"))).contains("7");
	}
}
