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
package eu.solven.adhoc.query.filter;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import eu.solven.adhoc.query.filter.value.RegexMatcher;
import eu.solven.adhoc.table.transcoder.ITableAliaser;
import eu.solven.adhoc.table.transcoder.value.ICustomTypeManagerSimple;

public class TestMoreFilterHelpers2 {

	// Transcodes by uppercasing string values; passes through non-string objects unchanged
	static final ICustomTypeManagerSimple UPPERCASE_MANAGER = new ICustomTypeManagerSimple() {
		@Override
		public boolean mayTranscode(String column) {
			return true;
		}

		@Override
		public Object toTable(String column, Object coordinate) {
			if (coordinate instanceof String s) {
				return s.toUpperCase();
			}
			return coordinate;
		}

		@Override
		public IValueMatcher toTable(String column, IValueMatcher valueMatcher) {
			return valueMatcher;
		}
	};

	// Manager that never transcodes any column
	static final ICustomTypeManagerSimple NO_TRANSCODE_MANAGER = new ICustomTypeManagerSimple() {
		@Override
		public boolean mayTranscode(String column) {
			return false;
		}

		@Override
		public Object toTable(String column, Object coordinate) {
			return coordinate;
		}

		@Override
		public IValueMatcher toTable(String column, IValueMatcher valueMatcher) {
			return valueMatcher;
		}
	};

	// -----------------------------------------------------------------------
	// transcodeType
	// -----------------------------------------------------------------------

	@Test
	public void transcodeType_noTranscode() {
		IValueMatcher matcher = EqualsMatcher.matchEq("foo");
		IValueMatcher result = MoreFilterHelpers.transcodeType(NO_TRANSCODE_MANAGER, "col", matcher);
		Assertions.assertThat(result).isSameAs(matcher);
	}

	@Test
	public void transcodeType_equalsMatcher() {
		IValueMatcher result = MoreFilterHelpers.transcodeType(UPPERCASE_MANAGER, "col", EqualsMatcher.matchEq("foo"));
		Assertions.assertThat(result).isEqualTo(EqualsMatcher.matchEq("FOO"));
	}

	@Test
	public void transcodeType_inMatcher() {
		IValueMatcher result =
				MoreFilterHelpers.transcodeType(UPPERCASE_MANAGER, "col", InMatcher.matchIn("foo", "bar"));
		Assertions.assertThat(result).isEqualTo(InMatcher.matchIn("FOO", "BAR"));
	}

	@Test
	public void transcodeType_notMatcher() {
		IValueMatcher inner = EqualsMatcher.matchEq("foo");
		IValueMatcher result = MoreFilterHelpers.transcodeType(UPPERCASE_MANAGER, "col", NotMatcher.not(inner));
		Assertions.assertThat(result).isEqualTo(NotMatcher.not(EqualsMatcher.matchEq("FOO")));
	}

	@Test
	public void transcodeType_nullMatcher_passthrough() {
		IValueMatcher matcher = NullMatcher.matchNull();
		IValueMatcher result = MoreFilterHelpers.transcodeType(UPPERCASE_MANAGER, "col", matcher);
		Assertions.assertThat(result).isSameAs(matcher);
	}

	@Test
	public void transcodeType_likeMatcher_passthrough() {
		IValueMatcher matcher = LikeMatcher.builder().pattern("foo%").build();
		IValueMatcher result = MoreFilterHelpers.transcodeType(UPPERCASE_MANAGER, "col", matcher);
		Assertions.assertThat(result).isSameAs(matcher);
	}

	@Test
	public void transcodeType_regexMatcher_passthrough() {
		IValueMatcher matcher = RegexMatcher.builder().pattern(RegexMatcher.compile("foo.*")).build();
		IValueMatcher result = MoreFilterHelpers.transcodeType(UPPERCASE_MANAGER, "col", matcher);
		Assertions.assertThat(result).isSameAs(matcher);
	}

	@Test
	public void transcodeType_andMatcher() {
		IValueMatcher and = AndMatcher.and(EqualsMatcher.matchEq("a"), EqualsMatcher.matchEq("b"));
		IValueMatcher result = MoreFilterHelpers.transcodeType(UPPERCASE_MANAGER, "col", and);
		Assertions.assertThat(result).isEqualTo(AndMatcher.and(EqualsMatcher.matchEq("A"), EqualsMatcher.matchEq("B")));
	}

	@Test
	public void transcodeType_orMatcher() {
		IValueMatcher or = OrMatcher.or(EqualsMatcher.matchEq("x"), EqualsMatcher.matchEq("y"));
		IValueMatcher result = MoreFilterHelpers.transcodeType(UPPERCASE_MANAGER, "col", or);
		Assertions.assertThat(result).isEqualTo(OrMatcher.or(EqualsMatcher.matchEq("X"), EqualsMatcher.matchEq("Y")));
	}

	// -----------------------------------------------------------------------
	// transcodeFilter
	// -----------------------------------------------------------------------

	@Test
	public void transcodeFilter_matchAll() {
		ISliceFilter result =
				MoreFilterHelpers.transcodeFilter(UPPERCASE_MANAGER, ITableAliaser.identity(), ISliceFilter.MATCH_ALL);
		Assertions.assertThat(result).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void transcodeFilter_matchNone() {
		ISliceFilter result =
				MoreFilterHelpers.transcodeFilter(UPPERCASE_MANAGER, ITableAliaser.identity(), ISliceFilter.MATCH_NONE);
		Assertions.assertThat(result).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void transcodeFilter_columnFilter() {
		ISliceFilter filter = ColumnFilter.matchEq("col", "foo");
		ISliceFilter result = MoreFilterHelpers.transcodeFilter(UPPERCASE_MANAGER, ITableAliaser.identity(), filter);
		Assertions.assertThat(result).isEqualTo(ColumnFilter.matchEq("col", "FOO"));
	}

	@Test
	public void transcodeFilter_andFilter() {
		ISliceFilter filter = AndFilter.and(Map.of("a", "va", "b", "vb"));
		ISliceFilter result = MoreFilterHelpers.transcodeFilter(UPPERCASE_MANAGER, ITableAliaser.identity(), filter);
		Assertions.assertThat(FilterHelpers.getFilteredColumns(result)).containsExactlyInAnyOrder("a", "b");
		Assertions.assertThat(FilterHelpers.getValueMatcher(result, "a")).isEqualTo(EqualsMatcher.matchEq("VA"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(result, "b")).isEqualTo(EqualsMatcher.matchEq("VB"));
	}

	@Test
	public void transcodeFilter_orFilter() {
		ISliceFilter filter = OrFilter.or(Map.of("a", "va", "b", "vb"));
		ISliceFilter result = MoreFilterHelpers.transcodeFilter(UPPERCASE_MANAGER, ITableAliaser.identity(), filter);
		Assertions.assertThat(result.isOr()).isTrue();
	}

	@Test
	public void transcodeFilter_notFilter() {
		ISliceFilter filter = ColumnFilter.matchEq("col", "foo").negate();
		ISliceFilter result = MoreFilterHelpers.transcodeFilter(UPPERCASE_MANAGER, ITableAliaser.identity(), filter);
		// ColumnFilter.negate() wraps value matcher with NotMatcher - isNot() stays false
		Assertions.assertThat(result.isColumnFilter()).isTrue();
		Assertions.assertThat(result).isEqualTo(ColumnFilter.matchEq("col", "FOO").negate());
	}

	// -----------------------------------------------------------------------
	// match(ISliceFilter, Map)
	// -----------------------------------------------------------------------

	@Test
	public void match_map_matchAll() {
		Assertions.assertThat(MoreFilterHelpers.match(ISliceFilter.MATCH_ALL, Map.of("a", "v"))).isTrue();
	}

	@Test
	public void match_map_columnEq_matches() {
		Assertions.assertThat(MoreFilterHelpers.match(ColumnFilter.matchEq("a", "v"), Map.of("a", "v"))).isTrue();
	}

	@Test
	public void match_map_columnEq_noMatch() {
		Assertions.assertThat(MoreFilterHelpers.match(ColumnFilter.matchEq("a", "other"), Map.of("a", "v"))).isFalse();
	}

	@Test
	public void match_column_value() {
		Assertions.assertThat(MoreFilterHelpers.match(ColumnFilter.matchEq("col", "x"), "col", "x")).isTrue();
		Assertions.assertThat(MoreFilterHelpers.match(ColumnFilter.matchEq("col", "x"), "col", "y")).isFalse();
	}

	@Test
	public void match_withTranscoder() {
		ISliceFilter filter = ColumnFilter.matchEq("col", "v");
		Assertions.assertThat(MoreFilterHelpers.match(ITableAliaser.identity(), filter, Map.of("col", "v"))).isTrue();
		Assertions.assertThat(MoreFilterHelpers.match(ITableAliaser.identity(), filter, Map.of("col", "other")))
				.isFalse();
	}
}
