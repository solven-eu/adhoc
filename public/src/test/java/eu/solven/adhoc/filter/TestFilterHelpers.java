/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.NotFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;

public class TestFilterHelpers {

	@Test
	public void testGetValueMatcher_in() {
		ColumnFilter inV1OrV2 = ColumnFilter.builder().column("c").matching(Set.of("v1", "v2")).build();
		Assertions.assertThat(FilterHelpers.getValueMatcher(inV1OrV2, "c")).isEqualTo(InMatcher.isIn("v1", "v2"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(inV1OrV2, "unknownColumn"))
				.isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_or() {
		IAdhocFilter like1OrLike2 = AndFilter.and(ColumnFilter.isLike("c1", "1%"), ColumnFilter.isLike("c2", "2%"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(like1OrLike2, "c1")).isEqualTo(LikeMatcher.matching("1%"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(like1OrLike2, "c2")).isEqualTo(LikeMatcher.matching("2%"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(like1OrLike2, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_ALL_NONE() {
		Assertions.assertThat(FilterHelpers.getValueMatcher(IAdhocFilter.MATCH_ALL, "c"))
				.isEqualTo(IValueMatcher.MATCH_ALL);
		Assertions.assertThat(FilterHelpers.getValueMatcher(IAdhocFilter.MATCH_NONE, "c"))
				.isEqualTo(IValueMatcher.MATCH_NONE);
	}

	@Test
	public void testGetValueMatcher_AND() {
		IAdhocFilter filter = AndFilter.and(Map.of("c1", "v1", "c2", "v2"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c1")).isEqualTo(EqualsMatcher.isEqualTo("v1"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c2")).isEqualTo(EqualsMatcher.isEqualTo("v2"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_Not() {
		IAdhocFilter filter = NotFilter.not(AndFilter.and(Map.of("c1", "v1", "c2", "v2")));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c1"))
				.isEqualTo(NotMatcher.not(EqualsMatcher.isEqualTo("v1")));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c2"))
				.isEqualTo(NotMatcher.not(EqualsMatcher.isEqualTo("v2")));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_Not_notOptimized() {
		IAdhocFilter filterNotAll = NotFilter.builder().negated(IAdhocFilter.MATCH_ALL).build();
		Assertions.assertThat(FilterHelpers.getValueMatcher(filterNotAll, "c1")).isEqualTo(IValueMatcher.MATCH_NONE);
		Assertions.assertThat(FilterHelpers.getValueMatcher(filterNotAll, "c2")).isEqualTo(IValueMatcher.MATCH_NONE);
		Assertions.assertThat(FilterHelpers.getValueMatcher(filterNotAll, "c3")).isEqualTo(IValueMatcher.MATCH_NONE);

		IAdhocFilter filterNotNone = NotFilter.builder().negated(IAdhocFilter.MATCH_NONE).build();
		Assertions.assertThat(FilterHelpers.getValueMatcher(filterNotNone, "c1")).isEqualTo(IValueMatcher.MATCH_ALL);
		Assertions.assertThat(FilterHelpers.getValueMatcher(filterNotNone, "c2")).isEqualTo(IValueMatcher.MATCH_ALL);
		Assertions.assertThat(FilterHelpers.getValueMatcher(filterNotNone, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_Not_inAnd() {
		IAdhocFilter filter =
				AndFilter.and(ColumnFilter.isEqualTo("c1", "v1"), NotFilter.not(ColumnFilter.isEqualTo("c2", "v2")));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c1")).isEqualTo(EqualsMatcher.isEqualTo("v1"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c2"))
				.isEqualTo(NotMatcher.not(EqualsMatcher.isEqualTo("v2")));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetValueMatcher_Not_matcher() {
		IAdhocFilter filter = AndFilter.and(Map.of("c1", "v1", "c2", NotMatcher.not(EqualsMatcher.isEqualTo("v2"))));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c1")).isEqualTo(EqualsMatcher.isEqualTo("v1"));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c2"))
				.isEqualTo(NotMatcher.not(EqualsMatcher.isEqualTo("v2")));
		Assertions.assertThat(FilterHelpers.getValueMatcher(filter, "c3")).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testGetFilteredColumns_and() {
		IAdhocFilter filter = AndFilter.and(Map.of("c1", "v1", "c2", "v2"));
		Assertions.assertThat(FilterHelpers.getFilteredColumns(filter)).isEqualTo(Set.of("c1", "c2"));
	}

	@Test
	public void testGetFilteredColumns_or() {
		IAdhocFilter filter = OrFilter.or(Map.of("c1", "v1", "c2", "v2"));
		Assertions.assertThat(FilterHelpers.getFilteredColumns(filter)).isEqualTo(Set.of("c1", "c2"));
	}

	@Test
	public void testGetFilteredColumns_not() {
		IAdhocFilter filter = NotFilter.not(OrFilter.or(Map.of("c1", "v1", "c2", "v2")));
		Assertions.assertThat(FilterHelpers.getFilteredColumns(filter)).isEqualTo(Set.of("c1", "c2"));
	}

	@Test
	public void testWrapToString() throws JsonProcessingException {
		IValueMatcher wrappedWithToString = FilterHelpers.wrapWithToString(v -> "foo".equals(v), () -> "someToString");

		Assertions.assertThat(wrappedWithToString.toString()).isEqualTo("someToString");
		Assertions.assertThat(new ObjectMapper().writeValueAsString(wrappedWithToString)).isEqualTo("""
				{"type":"FilterHelpers$1","wrapped":"someToString"}""");

		Assertions.assertThat(wrappedWithToString.match("foo")).isTrue();
		Assertions.assertThat(wrappedWithToString.match("bar")).isFalse();
	}

	@Test
	public void testAsMap_or() {
		Assertions.assertThat(FilterHelpers.asMap(ColumnFilter.isEqualTo("c", "v"))).isEqualTo(Map.of("c", "v"));

		// BEWARE We may introduce a `toList` to manage OR.
		Assertions
				.assertThatThrownBy(() -> FilterHelpers
						.asMap(OrFilter.or(ColumnFilter.isEqualTo("c1", "v1"), ColumnFilter.isEqualTo("c2", "v2"))))
				.isInstanceOf(IllegalArgumentException.class);

		Assertions.assertThatThrownBy(() -> FilterHelpers.asMap(IAdhocFilter.MATCH_NONE))
				.isInstanceOf(IllegalArgumentException.class);

	}

}
