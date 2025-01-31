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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.solven.adhoc.execute.FilterHelpers;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.NotFilter;
import eu.solven.adhoc.query.filter.value.LikeMatcher;

public class TestColumnFilter {

	/**
	 *
	 * @param key
	 * @param value
	 * @return a {@link Map} with given entry, accepting null key and value
	 */
	private static Map<String, Object> mapOfMayBeNull(String key, Object value) {
		Map<String, Object> explicitNull = new HashMap<>();
		explicitNull.put(key, value);
		return explicitNull;
	}

	@Test
	public void testMatchNull() {
		ColumnFilter kIsNull = ColumnFilter.builder().column("k").matchNull().build();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of())).isTrue();
		Assertions.assertThat(FilterHelpers.match(kIsNull, mapOfMayBeNull("k", null))).isTrue();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v"))).isFalse();
		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v2"))).isFalse();

		Assertions.assertThat(kIsNull.toString()).isEqualTo("k matches `NullMatcher()` (nullIfAbsent=true)");
	}

	@Test
	public void testMatchNullButNotMissing() {
		ColumnFilter kIsNull = ColumnFilter.builder().column("k").matchNull().nullIfAbsent(false).build();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of())).isFalse();
		Assertions.assertThat(FilterHelpers.match(kIsNull, mapOfMayBeNull("k", null))).isTrue();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v"))).isFalse();
		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v2"))).isFalse();
	}

	@Test
	public void testMatchNotNull() {
		ColumnFilter filterToNegate = ColumnFilter.builder().column("k").matchNull().build();
		NotFilter filter = NotFilter.builder().negated(filterToNegate).build();

		Assertions.assertThat(FilterHelpers.match(filter, Map.of())).isFalse();
		Assertions.assertThat(FilterHelpers.match(filter, mapOfMayBeNull("k", null))).isFalse();

		Assertions.assertThat(FilterHelpers.match(filter, Map.of("k", "v"))).isTrue();
		Assertions.assertThat(FilterHelpers.match(filterToNegate, Map.of("k", "v2"))).isFalse();
	}

	@Test
	public void testJavaUtilSetOf() {
		ColumnFilter filter = ColumnFilter.builder().column("k").matching(Set.of("v")).build();

		Assertions.assertThat(FilterHelpers.match(filter, Map.of())).isFalse();
		Assertions.assertThat(FilterHelpers.match(filter, mapOfMayBeNull("k", null))).isFalse();

		Assertions.assertThat(FilterHelpers.match(filter, Map.of("k", "v"))).isTrue();
		Assertions.assertThat(FilterHelpers.match(filter, Map.of("k", "v2"))).isFalse();

		Assertions.assertThat(filter.toString()).isEqualTo("k matches `InMatcher{size=1, #0=v}`");
	}

	@Test
	public void testCollectionsWithNull() {
		ColumnFilter filter = ColumnFilter.builder().column("k").matching(Arrays.asList(null, "v")).build();

		Assertions.assertThat(FilterHelpers.match(filter, Map.of())).isTrue();
		Assertions.assertThat(FilterHelpers.match(filter, mapOfMayBeNull("k", null))).isTrue();

		Assertions.assertThat(FilterHelpers.match(filter, Map.of("k", "v"))).isTrue();
		Assertions.assertThat(FilterHelpers.match(filter, Map.of("k", "v2"))).isFalse();
	}

	@Test
	public void testIsDistinctFrom() {
		ColumnFilter kIsNull = ColumnFilter.isDistinctFrom("k", "v");

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of())).isTrue();
		Assertions.assertThat(FilterHelpers.match(kIsNull, mapOfMayBeNull("k", null))).isTrue();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v"))).isFalse();
		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v2"))).isTrue();
	}

	@Test
	public void testIsIn_single() {
		IAdhocFilter kIsNull = ColumnFilter.isIn("k", "v");

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of())).isFalse();
		Assertions.assertThat(FilterHelpers.match(kIsNull, mapOfMayBeNull("k", null))).isFalse();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v"))).isTrue();
		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v2"))).isFalse();
	}

	@Test
	public void testIsIn_list() {
		IAdhocFilter kIsNull = ColumnFilter.isIn("k", List.of("v"));

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of())).isFalse();
		Assertions.assertThat(FilterHelpers.match(kIsNull, mapOfMayBeNull("k", null))).isFalse();

		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v"))).isTrue();
		Assertions.assertThat(FilterHelpers.match(kIsNull, Map.of("k", "v2"))).isFalse();
	}

	@Test
	public void testJackson_equals() throws JsonProcessingException {
		ColumnFilter ksEqualsV = ColumnFilter.isEqualTo("k", "v");

		ObjectMapper objectMapper = new ObjectMapper();

		String asString = objectMapper.writeValueAsString(ksEqualsV);
		Assertions.assertThat(asString).isEqualTo("""
				{"type":"column","column":"k","valueMatcher":{"type":"equals","operand":"v"},"nullIfAbsent":true}
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
				ColumnFilter.builder().column("a").matching(LikeMatcher.builder().like("prefix%").build()).build();

		Assertions.assertThat(ksEqualsV.isColumnFilter()).isTrue();
		Assertions.assertThat(ksEqualsV).isInstanceOfSatisfying(ColumnFilter.class, cf -> {
			Assertions.assertThat(cf.getValueMatcher()).isEqualTo(LikeMatcher.builder().like("prefix%").build());
		});
	}
}
