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

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.table.transcoder.IAdhocTableTranscoder;
import eu.solven.adhoc.table.transcoder.PrefixTranscoder;

public class TestFilterHelpers {
	@Test
	public void testIn() {
		ColumnFilter inV1OrV2 = ColumnFilter.builder().column("c").matching(Set.of("v1", "v2")).build();
		Assertions.assertThat(FilterHelpers.match(inV1OrV2, Map.of())).isFalse();
		Assertions.assertThat(FilterHelpers.match(inV1OrV2, Map.of("c", "v3"))).isFalse();

		Assertions.assertThat(FilterHelpers.match(inV1OrV2, Map.of("c", "v1"))).isTrue();
		Assertions.assertThat(FilterHelpers.match(inV1OrV2, Map.of("c", "v2"))).isTrue();
	}

	@Test
	public void testLike() {
		ColumnFilter startsWithV1 =
				ColumnFilter.builder().column("c").valueMatcher(LikeMatcher.builder().like("v1%").build()).build();

		Assertions.assertThat(FilterHelpers.match(startsWithV1, Map.of())).isFalse();
		Assertions.assertThat(FilterHelpers.match(startsWithV1, Map.of("c", "v3"))).isFalse();

		Assertions.assertThat(FilterHelpers.match(startsWithV1, Map.of("c", "v1"))).isTrue();
		Assertions.assertThat(FilterHelpers.match(startsWithV1, Map.of("c", "v2"))).isFalse();

		Assertions.assertThat(FilterHelpers.match(startsWithV1, Map.of("c", "p_v1"))).isFalse();
		Assertions.assertThat(FilterHelpers.match(startsWithV1, Map.of("c", "v1_s"))).isTrue();
	}

	@Test
	public void testIn_Transcoded() {
		IAdhocTableTranscoder transcoder = PrefixTranscoder.builder().prefix("p_").build();

		Assertions.assertThat(FilterHelpers.match(transcoder, ColumnFilter.isIn("c", "c1", "c2"), Map.of("p_c", "c1")))
				.isTrue();

		Assertions.assertThat(FilterHelpers.match(transcoder,
				AndFilter.and(ColumnFilter.isLike("c", "a%"), ColumnFilter.isLike("c", "%a")),
				Map.of("p_c", "a"))).isTrue();

		Assertions.assertThat(FilterHelpers.match(transcoder,
				OrFilter.or(ColumnFilter.isLike("c", "a%"), ColumnFilter.isLike("c", "%a")),
				Map.of("p_c", "azerty"))).isTrue();
	}
}
