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

import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.value.LikeMatcher;

public class TestColumnFilter {

	@Test
	public void testJackson_equals() throws JsonProcessingException {
		ColumnFilter ksEqualsV = ColumnFilter.isEqualTo("k", "v");

		ObjectMapper objectMapper = new ObjectMapper();

		String asString = objectMapper.writeValueAsString(ksEqualsV);
		Assertions.assertThat(asString).isEqualTo("""
				{"type":"column","column":"k","valueMatcher":"v","nullIfAbsent":true}
				""".strip());
		ColumnFilter fromString = objectMapper.readValue(asString, ColumnFilter.class);

		Assertions.assertThat(fromString).isEqualTo(ksEqualsV);
	}

	@Test
	public void testInEmpty() throws JsonProcessingException {
		IAdhocFilter ksEqualsV = ColumnFilter.isIn("k", Set.of());

		Assertions.assertThat(ksEqualsV).isEqualTo(IAdhocFilter.MATCH_NONE);
	}

	@Test
	public void testLikeMatcher() throws JsonProcessingException {
		IAdhocFilter ksEqualsV =
				ColumnFilter.builder().column("a").matching(LikeMatcher.builder().pattern("prefix%").build()).build();

		Assertions.assertThat(ksEqualsV.isColumnFilter()).isTrue();
		Assertions.assertThat(ksEqualsV).isInstanceOfSatisfying(ColumnFilter.class, cf -> {
			Assertions.assertThat(cf.getValueMatcher()).isEqualTo(LikeMatcher.builder().pattern("prefix%").build());
		});
	}
}
