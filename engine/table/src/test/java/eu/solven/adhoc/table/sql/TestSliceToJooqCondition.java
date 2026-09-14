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

import java.util.Optional;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.jooq.Condition;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.FilterBuilder;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.value.ComparingMatcher;
import eu.solven.adhoc.filter.value.InMatcher;
import eu.solven.adhoc.filter.value.LikeMatcher;
import eu.solven.adhoc.filter.value.NullMatcher;
import eu.solven.adhoc.table.sql.JooqTableQueryFactory.ConditionWithFilter;

/**
 * Unit tests for {@link SliceToJooqCondition}. Covers the value-matcher branches not exercised by the higher-level
 * {@code TestJooqTableQueryFactory_DuckDb}: {@code NullMatcher}, {@code InMatcher}, {@code LikeMatcher}, all four
 * {@link ComparingMatcher} variants, {@code matchAll}/{@code matchNone} short-circuits, the OR-with-nonPushdown path,
 * and the {@link SliceToJooqCondition#andSql} True-condition filter.
 */
public class TestSliceToJooqCondition {
	static {
		AdhocJooqHelper.disableBanners();
	}

	/** Simple name-mapping using DuckDB quoting conventions. */
	private final SliceToJooqCondition underTest =
			SliceToJooqCondition.builder().toName(col -> DSL.using(SQLDialect.DUCKDB).parser().parseName(col)).build();

	// ------------------------------------------------------------------
	// matchAll / matchNone
	// ------------------------------------------------------------------

	@Test
	public void testToCondition_matchAll_returnsTrueCondition() {
		ConditionWithFilter result = underTest.toConditionSplitNonPushdown(ISliceFilter.MATCH_ALL);

		Assertions.assertThat(result.getNonPushdown().isMatchAll()).isTrue();
		Assertions.assertThat(result.getCondition().toString()).isEqualTo("true");
	}

	@Test
	public void testToCondition_matchNone_returnsFalseCondition() {
		ConditionWithFilter result = underTest.toConditionSplitNonPushdown(ISliceFilter.MATCH_NONE);

		Assertions.assertThat(result.getCondition().toString()).isEqualTo("false");
	}

	// ------------------------------------------------------------------
	// NullMatcher
	// ------------------------------------------------------------------

	@Test
	public void testToCondition_NullMatcher() {
		ColumnFilter filter = ColumnFilter.match("c", NullMatcher.matchNull());
		Optional<Condition> condition = underTest.toCondition(filter);

		Assertions.assertThat(condition).isPresent();
		Assertions.assertThat(condition.get().toString()).isEqualTo("""
				"c" is null""");
	}

	// ------------------------------------------------------------------
	// LikeMatcher
	// ------------------------------------------------------------------

	@Test
	public void testToCondition_LikeMatcher() {
		ColumnFilter filter = ColumnFilter.matchLike("c", "hello%");
		Optional<Condition> condition = underTest.toCondition(filter);

		Assertions.assertThat(condition).isPresent();
		Assertions.assertThat(condition.get().toString()).isEqualTo("""
				"c" like 'hello%'""");
	}

	// ------------------------------------------------------------------
	// InMatcher
	// ------------------------------------------------------------------

	@Test
	public void testToCondition_InMatcher_twoValues() {
		ColumnFilter filter = ColumnFilter.matchIn("c", "v1", "v2");
		Optional<Condition> condition = underTest.toCondition(filter);

		Assertions.assertThat(condition).isPresent();
		Assertions.assertThat(condition.get().toString()).isEqualTo("""
				"c" in (
				  'v1', 'v2'
				)""");
	}

	@Test
	public void testToCondition_InMatcher_withNestedValueMatcher_throws() {
		// An InMatcher whose operand set contains another IValueMatcher is not supported.
		InMatcher inWithMatcher = InMatcher.builder().operand(NullMatcher.matchNull()).build();
		ColumnFilter filter = ColumnFilter.builder().column("c").valueMatcher(inWithMatcher).build();

		Assertions.assertThatThrownBy(() -> underTest.toCondition(filter))
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessageContaining("IValueMatcher");
	}

	// ------------------------------------------------------------------
	// ComparingMatcher — all four branches
	// ------------------------------------------------------------------

	@Test
	public void testToCondition_ComparingMatcher_strictlyGreaterThan() {
		ColumnFilter filter = ColumnFilter.match("c", ComparingMatcher.strictlyGreaterThan(10L));
		Optional<Condition> condition = underTest.toCondition(filter);

		Assertions.assertThat(condition).isPresent();
		Assertions.assertThat(condition.get().toString()).isEqualTo("""
				"c" > 10""");
	}

	@Test
	public void testToCondition_ComparingMatcher_greaterThanOrEqual() {
		ColumnFilter filter = ColumnFilter.match("c", ComparingMatcher.greaterThanOrEqual(10L));
		Optional<Condition> condition = underTest.toCondition(filter);

		Assertions.assertThat(condition).isPresent();
		Assertions.assertThat(condition.get().toString()).isEqualTo("""
				"c" >= 10""");
	}

	@Test
	public void testToCondition_ComparingMatcher_strictlyLowerThan() {
		ColumnFilter filter = ColumnFilter.match("c", ComparingMatcher.strictlyLowerThan(10L));
		Optional<Condition> condition = underTest.toCondition(filter);

		Assertions.assertThat(condition).isPresent();
		Assertions.assertThat(condition.get().toString()).isEqualTo("""
				"c" < 10""");
	}

	@Test
	public void testToCondition_ComparingMatcher_lowerThanOrEqual() {
		ColumnFilter filter = ColumnFilter.match("c", ComparingMatcher.lowerThanOrEqual(10L));
		Optional<Condition> condition = underTest.toCondition(filter);

		Assertions.assertThat(condition).isPresent();
		Assertions.assertThat(condition.get().toString()).isEqualTo("""
				"c" <= 10""");
	}

	// ------------------------------------------------------------------
	// OR with nonPushdown — OR falls back to matchAll condition
	// ------------------------------------------------------------------

	@Test
	public void testToCondition_OrWithNonPushdown_fallsBackToTrueCondition() {
		// One operand has a custom (non-pushdown) matcher: the OR cannot be pushed down at all.
		ColumnFilter customFilter = ColumnFilter.builder()
				.column("c")
				.valueMatcher(v -> v instanceof String s && s.startsWith("x"))
				.build();
		ISliceFilter orFilter = FilterBuilder.or(ColumnFilter.matchEq("d", "v1"), customFilter).combine();

		ConditionWithFilter result = underTest.toConditionSplitNonPushdown(orFilter);

		// The SQL side falls back to TRUE (no SQL filter applied for the OR).
		Assertions.assertThat(result.getCondition().toString()).isEqualTo("true");
		// The non-pushdown side carries the full OR back for post-filtering.
		Assertions.assertThat(result.getNonPushdown().isMatchAll()).isFalse();
	}

	// ------------------------------------------------------------------
	// andSql — True conditions are stripped
	// ------------------------------------------------------------------

	@Test
	public void testAndSql_allTrueConditions_returnsTrue() {
		// All-true conditions should collapse to a single TRUE.
		ConditionWithFilter result = underTest.and(Set.of(DSL.trueCondition(), DSL.trueCondition()),
				Set.of(ISliceFilter.MATCH_ALL, ISliceFilter.MATCH_ALL));

		Assertions.assertThat(result.getCondition().toString()).isEqualTo("true");
		Assertions.assertThat(result.getNonPushdown().isMatchAll()).isTrue();
	}

	@Test
	public void testAndSql_mixedTrueAndReal_stripsTrue() {
		// One TRUE and one real condition: the TRUE is dropped and only the real one remains.
		ConditionWithFilter result = underTest.and(Set.of(DSL.trueCondition(), DSL.field(DSL.quotedName("c")).eq("v")),
				Set.of(ISliceFilter.MATCH_ALL));

		Assertions.assertThat(result.getCondition().toString()).isEqualTo("""
				"c" = 'v'""");
	}

	// ------------------------------------------------------------------
	// wrap with hasParentNot adds IS NOT NULL guard
	// ------------------------------------------------------------------

	@Test
	public void testToCondition_withParentNot_addsIsNotNullGuard() {
		// Inside a NOT(…), equals must add an IS NOT NULL guard so that NULL rows are treated correctly.
		ColumnFilter filter = ColumnFilter.matchEq("c", "v1");
		Optional<Condition> condition = underTest.toCondition(filter, true);

		Assertions.assertThat(condition).isPresent();
		Assertions.assertThat(condition.get().toString()).isEqualTo("""
				(
				  "c" is not null
				  and "c" = 'v1'
				)""");
	}
}
