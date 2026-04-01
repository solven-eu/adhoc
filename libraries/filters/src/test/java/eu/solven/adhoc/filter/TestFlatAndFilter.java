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
package eu.solven.adhoc.filter;

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.filter.value.InMatcher;
import eu.solven.pepper.collection.MapWithNulls;

public class TestFlatAndFilter {

	// -------------------------------------------------------------------------
	// isMatchAll / isMatchNone / isAnd
	// -------------------------------------------------------------------------

	@Test
	public void testEmpty_isMatchAll() {
		ISliceFilter filter = FlatAndFilter.of(Map.of());

		Assertions.assertThat(filter.isMatchAll()).isTrue();
		Assertions.assertThat(filter.isMatchNone()).isFalse();
		Assertions.assertThat(filter.isAnd()).isTrue();

		Assertions.assertThat(filter).isEqualTo(ISliceFilter.MATCH_ALL);
		Assertions.assertThat(ISliceFilter.MATCH_ALL).isEqualTo(filter);
	}

	@Test
	public void testOneEntry_flags() {
		// of(1-entry) returns a ColumnFilter, which is not an AND node
		ISliceFilter filter = FlatAndFilter.of(Map.of("c1", "v1"));

		Assertions.assertThat(filter.isMatchAll()).isFalse();
		Assertions.assertThat(filter.isMatchNone()).isFalse();
		Assertions.assertThat(filter.isAnd()).isFalse();
	}

	@Test
	public void testTwoEntries_flags() {
		FlatAndFilter filter = (FlatAndFilter) FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2"));

		Assertions.assertThat(filter.isMatchAll()).isFalse();
		Assertions.assertThat(filter.isMatchNone()).isFalse();
		Assertions.assertThat(filter.isAnd()).isTrue();
	}

	// -------------------------------------------------------------------------
	// getOperands
	// -------------------------------------------------------------------------

	@Test
	public void testEmpty_getOperands() {
		IAndFilter filter = (IAndFilter) FlatAndFilter.of(Map.of());

		Assertions.assertThat(filter.getOperands()).isEmpty();
	}

	@Test
	public void testOneEntry_getOperands() {
		// of(1-entry) returns the raw ColumnFilter — no AND wrapper
		ISliceFilter filter = FlatAndFilter.of(Map.of("c1", "v1"));

		Assertions.assertThat(filter).isEqualTo(ColumnFilter.matchEq("c1", "v1"));
	}

	@Test
	public void testOneEntry_nullValue() {
		Map<String, Object> mapWithNull = MapWithNulls.<String, Object>builder().put("c1", null).build();
		ISliceFilter filter = FlatAndFilter.of(mapWithNull);

		Assertions.assertThat(filter).isEqualTo(ColumnFilter.builder().column("c1").matchNull().build());
	}

	@Test
	public void testOneEntry_valueMatcher_isUsedAsIs() {
		// When the value is already an IValueMatcher, of() must pass it through rather than wrapping it in an
		// EqualsMatcher — Map.of rejects null so we use a concrete non-equality matcher.
		IValueMatcher inMatcher = InMatcher.matchIn(java.util.List.of("v1", "v2"));
		ISliceFilter filter = FlatAndFilter.of(Map.of("c1", inMatcher));

		Assertions.assertThat(filter).isInstanceOf(ColumnFilter.class);
		Assertions.assertThat(((ColumnFilter) filter).getValueMatcher()).isSameAs(inMatcher);
	}

	@Test
	public void testTwoEntries_getOperands() {
		FlatAndFilter filter = (FlatAndFilter) FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2"));

		Set<IColumnFilter> operands = filter.getOperands();
		Assertions.assertThat(operands).hasSize(2);
		Assertions.assertThat((Set) operands)
				.containsExactlyInAnyOrder(ColumnFilter.matchEq("c1", "v1"), ColumnFilter.matchEq("c2", "v2"));
	}

	// -------------------------------------------------------------------------
	// toString
	// -------------------------------------------------------------------------

	@Test
	public void testEmpty_toString() {
		Assertions.assertThat(FlatAndFilter.of(Map.of()).toString()).isEqualTo("matchAll");
	}

	@Test
	public void testOneEntry_toString() {
		Assertions.assertThat(FlatAndFilter.of(Map.of("c1", "v1")).toString()).isEqualTo("c1==v1");
	}

	@Test
	public void testTwoEntries_toString() {
		// ImmutableMap preserves insertion order
		FlatAndFilter filter = (FlatAndFilter) FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2"));
		// Both orderings are valid; just check both columns appear
		Assertions.assertThat(filter.toString()).contains("c1==v1").contains("c2==v2").contains("&");
	}

	// -------------------------------------------------------------------------
	// negate
	// -------------------------------------------------------------------------

	@Test
	public void testNegate() {
		// FlatAndFilter.negate() wraps in a NOT node; use 2-entry to get a FlatAndFilter
		FlatAndFilter filter = (FlatAndFilter) FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2"));
		ISliceFilter negated = filter.negate();

		Assertions.assertThat(negated.isNot()).isTrue();
	}

	// -------------------------------------------------------------------------
	// equals — same type
	// -------------------------------------------------------------------------

	@Test
	public void testEquals_sameType_empty() {
		Assertions.assertThat(FlatAndFilter.of(Map.of())).isEqualTo(FlatAndFilter.of(Map.of()));
	}

	@Test
	public void testEquals_sameType_oneEntry() {
		Assertions.assertThat(FlatAndFilter.of(Map.of("c1", "v1"))).isEqualTo(FlatAndFilter.of(Map.of("c1", "v1")));
	}

	@Test
	public void testEquals_sameType_twoEntries() {
		Assertions.assertThat(FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2")))
				.isEqualTo(FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2")));
	}

	@Test
	public void testNotEquals_sameType_differentValue() {
		Assertions.assertThat(FlatAndFilter.of(Map.of("c1", "v1"))).isNotEqualTo(FlatAndFilter.of(Map.of("c1", "v2")));
	}

	@Test
	public void testNotEquals_sameType_differentColumn() {
		Assertions.assertThat(FlatAndFilter.of(Map.of("c1", "v1"))).isNotEqualTo(FlatAndFilter.of(Map.of("c2", "v1")));
	}

	// -------------------------------------------------------------------------
	// equals — cross-type with AndFilter / IAndFilter
	// -------------------------------------------------------------------------

	@Test
	public void testEquals_crossType_empty_vsMatchAll() {
		// of(empty) returns MATCH_ALL directly
		Assertions.assertThat(FlatAndFilter.of(Map.of())).isSameAs(ISliceFilter.MATCH_ALL);
		Assertions.assertThat(FlatAndFilter.of(Map.of()).isMatchAll()).isTrue();
	}

	@Test
	public void testEquals_crossType_oneEntry_vsColumnFilter() {
		// of(1-entry) returns the ColumnFilter directly — no AND wrapper
		Assertions.assertThat(FlatAndFilter.of(Map.of("c1", "v1"))).isEqualTo(ColumnFilter.matchEq("c1", "v1"));
	}

	@Test
	public void testEquals_crossType_twoEntries_vsAndFilter() {
		IAndFilter andFilter =
				AndFilter.copyOf(Set.of(ColumnFilter.matchEq("c1", "v1"), ColumnFilter.matchEq("c2", "v2")));

		Assertions.assertThat(FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2"))).isEqualTo(andFilter);
	}

	@Test
	public void testNotEquals_crossType_differentValue() {
		// 2-entry FlatAndFilter vs AndFilter with a different value
		IAndFilter andFilter =
				AndFilter.copyOf(Set.of(ColumnFilter.matchEq("c1", "v2"), ColumnFilter.matchEq("c2", "v2")));

		Assertions.assertThat(FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2"))).isNotEqualTo(andFilter);
	}

	@Test
	public void testNotEquals_crossType_extraColumn() {
		IAndFilter andFilter =
				AndFilter.copyOf(Set.of(ColumnFilter.matchEq("c1", "v1"), ColumnFilter.matchEq("c2", "v2")));

		Assertions.assertThat(FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2", "c3", "v3"))).isNotEqualTo(andFilter);
	}

	@Test
	public void testNotEquals_crossType_nonEqualsMatcher() {
		// AndFilter with a non-EqualsMatcher operand — should not be equal to a FlatAndFilter
		IAndFilter andFilter = AndFilter
				.copyOf(Set.of(ColumnFilter.match("c1", IValueMatcher.MATCH_ALL), ColumnFilter.matchEq("c2", "v2")));

		Assertions.assertThat(FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2"))).isNotEqualTo(andFilter);
	}

	// -------------------------------------------------------------------------
	// hashCode consistency (same type)
	// -------------------------------------------------------------------------

	@Test
	public void testHashCode_consistent_sameType() {
		ISliceFilter f1 = FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2"));
		ISliceFilter f2 = FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2"));

		Assertions.assertThat(f1.hashCode()).isEqualTo(f2.hashCode());
	}

	@Test
	public void testHashCode_compatible_withAndFilter() {
		ISliceFilter simple = FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2"));
		IAndFilter and = AndFilter.copyOf(Set.of(ColumnFilter.matchEq("c1", "v1"), ColumnFilter.matchEq("c2", "v2")));

		Assertions.assertThat(simple.hashCode()).isEqualTo(and.hashCode());
	}

	// -------------------------------------------------------------------------
	// null-value handling (e.g. slice from a failed JOIN)
	// -------------------------------------------------------------------------

	@Test
	public void testOf_nullValue_doesNotThrow() {
		// null from a failed JOIN: of() must not throw but delegate to AndFilter
		Map<String, Object> mapWithNull =
				MapWithNulls.<String, Object>builder().put("c1", "v1").put("c2", null).build();
		ISliceFilter filter = FlatAndFilter.of(mapWithNull);

		Assertions.assertThat(filter).isNotNull();
		Assertions.assertThat(filter.isAnd()).isTrue();
		Assertions.assertThat(filter.isMatchAll()).isFalse();
		Assertions.assertThat(filter.isMatchNone()).isFalse();
	}

	@Test
	public void testOf_nullValue_equalsAndFilter() {
		Map<String, Object> mapWithNull =
				MapWithNulls.<String, Object>builder().put("c1", "v1").put("c2", null).build();

		Assertions.assertThat(FlatAndFilter.of(mapWithNull)).isEqualTo(AndFilter.and(mapWithNull));
	}

	@Test
	public void testOf_nullValue_hashCode_compatibleWithAndFilter() {
		Map<String, Object> mapWithNull =
				MapWithNulls.<String, Object>builder().put("c1", "v1").put("c2", null).build();

		Assertions.assertThat(FlatAndFilter.of(mapWithNull).hashCode())
				.isEqualTo(AndFilter.and(mapWithNull).hashCode());
	}

	@Test
	public void testOf_nullValue_isFlatAndFilter() {
		Map<String, Object> mapWithNull =
				MapWithNulls.<String, Object>builder().put("c1", "v1").put("c2", null).build();

		Assertions.assertThat(FlatAndFilter.of(mapWithNull)).isInstanceOf(FlatAndFilter.class);
	}

	@Test
	public void testOf_nullValue_toString() {
		// ImmutableMap preserves insertion order; use a builder to control it
		Map<String, Object> mapWithNull =
				MapWithNulls.<String, Object>builder().put("c1", "v1").put("c2", null).build();

		Assertions.assertThat(FlatAndFilter.of(mapWithNull).toString()).contains("c1==v1").contains("c2 IS NULL");
	}

	@Test
	public void testOf_nullValue_getOperands() {
		Map<String, Object> mapWithNull =
				MapWithNulls.<String, Object>builder().put("c1", "v1").put("c2", null).build();

		Set<? extends ISliceFilter> operands = ((IAndFilter) FlatAndFilter.of(mapWithNull)).getOperands();

		Assertions.assertThat((Set) operands)
				.containsExactlyInAnyOrder(ColumnFilter.matchEq("c1", "v1"),
						ColumnFilter.builder().column("c2").matchNull().build());
	}

	// -------------------------------------------------------------------------
	// FilterHelpers integration
	// -------------------------------------------------------------------------

	@Test
	public void testSplitAnd_empty() {
		Assertions.assertThat(FilterHelpers.splitAnd(FlatAndFilter.of(Map.of()))).isEmpty();
	}

	@Test
	public void testSplitAnd_oneEntry() {
		Set<ISliceFilter> split = FilterHelpers.splitAnd(FlatAndFilter.of(Map.of("c1", "v1")));

		Assertions.assertThat(split).containsExactly(ColumnFilter.matchEq("c1", "v1"));
	}

	@Test
	public void testSplitAnd_twoEntries() {
		Set<ISliceFilter> split = FilterHelpers.splitAnd(FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2")));

		Assertions.assertThat(split)
				.containsExactlyInAnyOrder(ColumnFilter.matchEq("c1", "v1"), ColumnFilter.matchEq("c2", "v2"));
	}

	@Test
	public void testAsMap_empty() {
		Assertions.assertThat(FilterHelpers.asMap(FlatAndFilter.of(Map.of()))).isEmpty();
	}

	@Test
	public void testAsMap_twoEntries() {
		Map<String, Object> result = FilterHelpers.asMap(FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2")));

		Assertions.assertThat(result).containsEntry("c1", "v1").containsEntry("c2", "v2").hasSize(2);
	}

	@Test
	public void getValueMatcherLax() {
		ISliceFilter filter = FlatAndFilter.of(Map.of("c1", "v1", "c2", "v2"));

		Assertions.assertThat(FilterHelpers.getValueMatcherLax(filter, "c1")).isEqualTo(EqualsMatcher.matchEq("v1"));
		Assertions.assertThat(FilterHelpers.getValueMatcherLax(filter, "c2")).isEqualTo(EqualsMatcher.matchEq("v2"));
		Assertions.assertThat(FilterHelpers.getValueMatcherLax(filter, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}
}
