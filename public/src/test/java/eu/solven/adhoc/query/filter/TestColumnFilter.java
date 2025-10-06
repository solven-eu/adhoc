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
package eu.solven.adhoc.query.filter;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import eu.solven.adhoc.query.filter.value.SameMatcher;

public class TestColumnFilter {

	@Test
	public void testJackson_equals() throws JsonProcessingException {
		ISliceFilter ksEqualsV = ColumnFilter.matchEq("k", "v");

		ObjectMapper objectMapper = new ObjectMapper();

		String asString = objectMapper.writeValueAsString(ksEqualsV);
		Assertions.assertThat(asString).isEqualTo("""
				{"type":"column","column":"k","valueMatcher":"v","nullIfAbsent":true}
				""".strip());
		ISliceFilter fromString = objectMapper.readValue(asString, ISliceFilter.class);

		Assertions.assertThat(fromString).isEqualTo(ksEqualsV);
	}

	@Test
	public void testInEmpty() {
		ISliceFilter ksEqualsV = ColumnFilter.matchIn("k", Set.of());

		Assertions.assertThat(ksEqualsV).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testLikeMatcher() {
		ISliceFilter filter =
				ColumnFilter.builder().column("a").matching(LikeMatcher.builder().pattern("prefix%").build()).build();

		Assertions.assertThat(filter.isColumnFilter()).isTrue();
		Assertions.assertThat(filter).isInstanceOfSatisfying(ColumnFilter.class, cf -> {
			Assertions.assertThat(cf.getValueMatcher()).isEqualTo(LikeMatcher.builder().pattern("prefix%").build());
		});
	}

	@Test
	public void testToString_equals() {
		ISliceFilter filter = ColumnFilter.matchEq("c", "v");

		Assertions.assertThat(filter).hasToString("c==v");
	}

	@Test
	public void testToString_equals_not() {
		ISliceFilter filter =
				ColumnFilter.builder().column("c").matching(NotMatcher.not(EqualsMatcher.matchEq("v"))).build();

		Assertions.assertThat(filter).hasToString("c!=v");
	}

	@Test
	public void testToString_same() {
		ISliceFilter filter =
				ColumnFilter.builder().column("c").matching(SameMatcher.builder().operand("v").build()).build();

		Assertions.assertThat(filter).hasToString("c===v");
	}

	@Test
	public void testToString_same_not() {
		ISliceFilter filter = ColumnFilter.builder()
				.column("c")
				.matching(NotMatcher.not(SameMatcher.builder().operand("v").build()))
				.build();

		Assertions.assertThat(filter).hasToString("c!==v");
	}

	@Test
	public void testToString_null() {
		ISliceFilter filter = ColumnFilter.builder().column("c").matching(NullMatcher.matchNull()).build();

		Assertions.assertThat(filter).hasToString("c===null");
	}

	@Test
	public void testToString_null_not() {
		ISliceFilter filter =
				ColumnFilter.builder().column("c").matching(NotMatcher.not(NullMatcher.matchNull())).build();

		Assertions.assertThat(filter).hasToString("c!==null");
	}

	@Test
	public void testToString_in() {
		ISliceFilter filter = ColumnFilter.matchIn("c", ImmutableSet.of("v1", "v2"));

		Assertions.assertThat(filter).hasToString("c=in=(v1,v2)");
	}

	@Test
	public void testToString_out() {
		ISliceFilter filter =
				ColumnFilter.builder().column("c").matching(NotMatcher.not(InMatcher.matchIn("v1", "v2"))).build();

		Assertions.assertThat(filter).hasToString("c=out=(v1,v2)");
	}

	@Test
	public void testToString_likes() {
		ISliceFilter ksEqualsV = ColumnFilter.builder().column("c").matching(LikeMatcher.matching("a%")).build();

		Assertions.assertThat(ksEqualsV).hasToString("c matches `LikeMatcher(pattern=a%)`");
	}

	@Test
	public void testToString_not_likes() {
		ISliceFilter ksEqualsV =
				ColumnFilter.builder().column("c").matching(NotMatcher.not(LikeMatcher.matching("a%"))).build();

		Assertions.assertThat(ksEqualsV).hasToString("c does NOT match `LikeMatcher(pattern=a%)`");
	}

	@Test
	public void testMatchAll() {
		ISliceFilter cIsMatchAll = ColumnFilter.match("c", IValueMatcher.MATCH_ALL);

		Assertions.assertThat(cIsMatchAll).hasToString("matchAll");
	}

	@Test
	public void testEqualTo_matcher() {
		ISliceFilter cIsMatchAll = ColumnFilter.matchEq("c", LikeMatcher.matching("a%"));

		Assertions.assertThat(cIsMatchAll).hasToString("c matches `LikeMatcher(pattern=a%)`");
	}

	@Test
	public void testEqualTo_Set() {
		ISliceFilter cIsMatchAll = ColumnFilter.matchEq("c", ImmutableSet.of("v1", "v2"));

		Assertions.assertThat(cIsMatchAll).hasToString("c==[v1, v2]");
	}
}
