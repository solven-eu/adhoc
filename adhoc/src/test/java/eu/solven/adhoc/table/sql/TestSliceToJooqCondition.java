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
package eu.solven.adhoc.table.sql;

import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.value.ComparingMatcher;
import eu.solven.adhoc.filter.value.LikeMatcher;
import eu.solven.adhoc.filter.value.NotMatcher;
import eu.solven.adhoc.filter.value.NullMatcher;
import eu.solven.adhoc.filter.value.StringMatcher;
import eu.solven.adhoc.table.sql.JooqTableQueryFactory.ConditionWithFilter;

public class TestSliceToJooqCondition {

	final SliceToJooqCondition sut = SliceToJooqCondition.builder().toName(DSL::name).build();

	@Test
	public void testMatchAll_returnsTrue() {
		ConditionWithFilter result = sut.toCondition(ISliceFilter.MATCH_ALL);

		Assertions.assertThat(result.getCondition().toString()).isEqualTo(DSL.trueCondition().toString());
		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
	}

	@Test
	public void testMatchNone_returnsFalse() {
		ConditionWithFilter result = sut.toCondition(ISliceFilter.MATCH_NONE);

		Assertions.assertThat(result.getCondition().toString()).isEqualTo(DSL.falseCondition().toString());
		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
	}

	@Test
	public void testColumnFilter_equals_generatesEqCondition() {
		ISliceFilter filter = ColumnFilter.matchEq("col", "value");

		ConditionWithFilter result = sut.toCondition(filter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col").contains("value");
	}

	@Test
	public void testColumnFilter_isNull_generatesIsNullCondition() {
		ISliceFilter filter = ColumnFilter.builder().column("col").valueMatcher(NullMatcher.instance()).build();

		ConditionWithFilter result = sut.toCondition(filter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col").containsIgnoringCase("null");
	}

	@Test
	public void testColumnFilter_in_generatesInCondition() {
		ISliceFilter filter = ColumnFilter.matchIn("col", Set.of("a", "b"));

		ConditionWithFilter result = sut.toCondition(filter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col").containsIgnoringCase("in");
	}

	@Test
	public void testColumnFilter_like_generatesLikeCondition() {
		ISliceFilter filter = ColumnFilter.builder()
				.column("col")
				.valueMatcher(LikeMatcher.builder().pattern("val%").build())
				.build();

		ConditionWithFilter result = sut.toCondition(filter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col").containsIgnoringCase("like");
	}

	@Test
	public void testColumnFilter_stringMatcher_generatesCastEqCondition() {
		ISliceFilter filter = ColumnFilter.builder()
				.column("col")
				.valueMatcher(StringMatcher.builder().string("val").build())
				.build();

		ConditionWithFilter result = sut.toCondition(filter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col");
	}

	@Test
	public void testColumnFilter_comparingMatcher_greaterThan() {
		ISliceFilter filter = ColumnFilter.builder()
				.column("col")
				.valueMatcher(ComparingMatcher.builder().greaterThan(true).matchIfEqual(false).operand(10).build())
				.build();

		ConditionWithFilter result = sut.toCondition(filter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col").contains("10");
	}

	@Test
	public void testColumnFilter_comparingMatcher_greaterOrEqual() {
		ISliceFilter filter = ColumnFilter.builder()
				.column("col")
				.valueMatcher(ComparingMatcher.builder().greaterThan(true).matchIfEqual(true).operand(5).build())
				.build();

		ConditionWithFilter result = sut.toCondition(filter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col").contains("5");
	}

	@Test
	public void testColumnFilter_comparingMatcher_lessThan() {
		ISliceFilter filter = ColumnFilter.builder()
				.column("col")
				.valueMatcher(ComparingMatcher.builder().greaterThan(false).matchIfEqual(false).operand(100).build())
				.build();

		ConditionWithFilter result = sut.toCondition(filter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col").contains("100");
	}

	@Test
	public void testColumnFilter_comparingMatcher_lessOrEqual() {
		ISliceFilter filter = ColumnFilter.builder()
				.column("col")
				.valueMatcher(ComparingMatcher.builder().greaterThan(false).matchIfEqual(true).operand(7).build())
				.build();

		ConditionWithFilter result = sut.toCondition(filter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col").contains("7");
	}

	@Test
	public void testNotFilter_eq_generatesNotCondition() {
		ISliceFilter eqFilter = ColumnFilter.matchEq("col", "val");
		ISliceFilter notFilter = eqFilter.negate();

		ConditionWithFilter result = sut.toCondition(notFilter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col");
	}

	@Test
	public void testAndFilter_bothColumnFilters_combinesWithAnd() {
		ISliceFilter andFilter =
				FilterBuilder.and(List.of(ColumnFilter.matchEq("col1", "v1"), ColumnFilter.matchEq("col2", "v2")))
						.optimize();

		ConditionWithFilter result = sut.toCondition(andFilter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col1").contains("col2");
	}

	@Test
	public void testOrFilter_allColumnFilters_combinesWithOr() {
		ISliceFilter orFilter =
				FilterBuilder.or(List.of(ColumnFilter.matchEq("col", "v1"), ColumnFilter.matchEq("col", "v2")))
						.optimize();

		ConditionWithFilter result = sut.toCondition(orFilter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col");
	}

	@Test
	public void testColumnFilter_notMatcher_generatesNotCondition() {
		ISliceFilter filter = ColumnFilter.builder()
				.column("col")
				.valueMatcher(NotMatcher.builder()
						.negated(eu.solven.adhoc.filter.value.EqualsMatcher.builder().operand("val").build())
						.build())
				.build();

		ConditionWithFilter result = sut.toCondition(filter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col");
	}

	@Test
	public void testWrap_hasParentNot_addsIsNotNull() {
		// When hasParentNot=true, wrap adds an IS NOT NULL check alongside the condition
		ISliceFilter notFilter = ColumnFilter.matchEq("col", "val").negate();

		ConditionWithFilter result = sut.toCondition(notFilter);

		// The NOT wrapping ensures null-safety: col IS NOT NULL AND NOT(col = 'val')
		Assertions.assertThat(result.getCondition().toString()).containsIgnoringCase("not null").contains("col");
	}

	@Test
	public void testToConditionSplitLeftover_matchAll_returnsTrue() {
		ConditionWithFilter result = sut.toConditionSplitLeftover(ISliceFilter.MATCH_ALL);

		Assertions.assertThat(result.getCondition().toString()).isEqualTo(DSL.trueCondition().toString());
		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
	}

	@Test
	public void testToConditionSplitLeftover_columnFilter_noLeftover() {
		ISliceFilter filter = ColumnFilter.matchEq("col", "val");

		ConditionWithFilter result = sut.toConditionSplitLeftover(filter);

		Assertions.assertThat(result.getLeftover().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).contains("col").contains("val");
	}
}
