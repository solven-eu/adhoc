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

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import eu.solven.adhoc.query.filter.value.SameMatcher;

public class TestColumnFilter {

	@Test
	public void testJackson_equals() throws JsonProcessingException {
		ColumnFilter ksEqualsV = ColumnFilter.isEqualTo("k", "v");

		ObjectMapper objectMapper = new ObjectMapper();

		String asString = objectMapper.writeValueAsString(ksEqualsV);
		Assertions.assertThat(asString).isEqualTo("""
				{"type":"column","column":"k","valueMatcher":"v","nullIfAbsent":true}
				""".strip());
		IAdhocFilter fromString = objectMapper.readValue(asString, IAdhocFilter.class);

		Assertions.assertThat(fromString).isEqualTo(ksEqualsV);
	}

	@Test
	public void testInEmpty() {
		IAdhocFilter ksEqualsV = ColumnFilter.isIn("k", Set.of());

		Assertions.assertThat(ksEqualsV).isEqualTo(IAdhocFilter.MATCH_NONE);
	}

	@Test
	public void testLikeMatcher() {
		IAdhocFilter filter =
				ColumnFilter.builder().column("a").matching(LikeMatcher.builder().pattern("prefix%").build()).build();

		Assertions.assertThat(filter.isColumnFilter()).isTrue();
		Assertions.assertThat(filter).isInstanceOfSatisfying(ColumnFilter.class, cf -> {
			Assertions.assertThat(cf.getValueMatcher()).isEqualTo(LikeMatcher.builder().pattern("prefix%").build());
		});
	}

	@Test
	public void testToString_equals() {
		IAdhocFilter filter = ColumnFilter.isEqualTo("c", "v");

		Assertions.assertThat(filter).hasToString("c==v");
	}

	@Test
	public void testToString_equals_not() {
		IAdhocFilter filter =
				ColumnFilter.builder().column("c").matching(NotMatcher.not(EqualsMatcher.isEqualTo("v"))).build();

		Assertions.assertThat(filter).hasToString("c!=v");
	}

	@Test
	public void testToString_same() {
		IAdhocFilter filter =
				ColumnFilter.builder().column("c").matching(SameMatcher.builder().operand("v").build()).build();

		Assertions.assertThat(filter).hasToString("c===v");
	}

	@Test
	public void testToString_same_not() {
		IAdhocFilter filter = ColumnFilter.builder()
				.column("c")
				.matching(NotMatcher.not(SameMatcher.builder().operand("v").build()))
				.build();

		Assertions.assertThat(filter).hasToString("c!==v");
	}

	@Test
	public void testToString_null() {
		IAdhocFilter filter = ColumnFilter.builder().column("c").matching(NullMatcher.matchNull()).build();

		Assertions.assertThat(filter).hasToString("c===null");
	}

	@Test
	public void testToString_null_not() {
		IAdhocFilter filter =
				ColumnFilter.builder().column("c").matching(NotMatcher.not(NullMatcher.matchNull())).build();

		Assertions.assertThat(filter).hasToString("c!==null");
	}

	@Test
	public void testToString_in() {
		IAdhocFilter filter = ColumnFilter.isIn("c", ImmutableSet.of("v1", "v2"));

		Assertions.assertThat(filter).hasToString("c=in=(v1,v2)");
	}

	@Test
	public void testToString_out() {
		IAdhocFilter filter =
				ColumnFilter.builder().column("c").matching(NotMatcher.not(InMatcher.isIn("v1", "v2"))).build();

		Assertions.assertThat(filter).hasToString("c=out=(v1,v2)");
	}

	@Test
	public void testToString_likes() {
		IAdhocFilter ksEqualsV = ColumnFilter.builder().column("c").matching(LikeMatcher.matching("a%")).build();

		Assertions.assertThat(ksEqualsV).hasToString("c matches `LikeMatcher(pattern=a%)`");
	}

	@Test
	public void testToString_not_likes() {
		IAdhocFilter ksEqualsV =
				ColumnFilter.builder().column("c").matching(NotMatcher.not(LikeMatcher.matching("a%"))).build();

		Assertions.assertThat(ksEqualsV).hasToString("c does NOT match `LikeMatcher(pattern=a%)`");
	}
}
