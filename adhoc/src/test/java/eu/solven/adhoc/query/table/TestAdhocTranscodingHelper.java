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
package eu.solven.adhoc.query.table;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.NotFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.table.transcoder.AdhocTranscodingHelper;
import eu.solven.adhoc.table.transcoder.IAdhocTableTranscoder;
import eu.solven.adhoc.table.transcoder.MapTableTranscoder;
import eu.solven.adhoc.table.transcoder.PrefixTranscoder;
import eu.solven.adhoc.table.transcoder.TranscodingContext;

public class TestAdhocTranscodingHelper {
	@Test
	public void testSimpleMapping_Empty() {
		IAdhocTableTranscoder transcoder = MapTableTranscoder.builder().build();
		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("k")).isEqualTo("k");

		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of())).isEmpty();
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("k", "v")))
				.isEqualTo(Map.of("k", "v"));
		// Receiving unknown column would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("otherK", "v")))
				.isEqualTo(Map.of("otherK", "v"));
	}

	@Test
	public void testSimpleMapping() {
		IAdhocTableTranscoder transcoder = MapTableTranscoder.builder().queriedToUnderlying("k1", "k").build();
		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("k1")).isEqualTo("k");
		Assertions.assertThat(context.underlying("k2")).isEqualTo("k2");

		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of())).isEmpty();
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("k", "v")))
				.isEqualTo(Map.of("k1", "v"));
		// Receiving unknown column would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("otherK", "v")))
				.isEqualTo(Map.of("otherK", "v"));
	}

	@Test
	public void testSimpleMapping_RequestUnderlying() {
		IAdhocTableTranscoder transcoder = MapTableTranscoder.builder().queriedToUnderlying("k1", "k").build();
		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("k1")).isEqualTo("k");
		Assertions.assertThat(context.underlying("k")).isEqualTo("k");

		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of())).isEmpty();
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("k", "v")))
				.isEqualTo(Map.of("k1", "v", "k", "v"));
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("k1", "v")))
				.isEqualTo(Map.of("k1", "v"));
		// Receiving unknown column would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("otherK", "v")))
				.isEqualTo(Map.of("otherK", "v"));
	}

	@Test
	public void testOverlap() {
		IAdhocTableTranscoder transcoder =
				MapTableTranscoder.builder().queriedToUnderlying("k1", "k2").queriedToUnderlying("k2", "k3").build();
		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("k1")).isEqualTo("k2");
		Assertions.assertThat(context.underlying("k2")).isEqualTo("k3");

		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of())).isEmpty();
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("k2", "v")))
				.isEqualTo(Map.of("k1", "v"));
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("k3", "v")))
				.isEqualTo(Map.of("k2", "v"));

		// Receiving k1 from DB would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("k1", "v")))
				.isEqualTo(Map.of("k1", "v"));
		// Receiving unknown column would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("otherK", "v")))
				.isEqualTo(Map.of("otherK", "v"));
	}

	@Test
	public void testIn() {
		ColumnFilter inV1OrV2 = ColumnFilter.builder().column("c").matching(Set.of("v1", "v2")).build();
		Assertions.assertThat(AdhocTranscodingHelper.match(inV1OrV2, Map.of())).isFalse();
		Assertions.assertThat(AdhocTranscodingHelper.match(inV1OrV2, Map.of("c", "v3"))).isFalse();

		Assertions.assertThat(AdhocTranscodingHelper.match(inV1OrV2, Map.of("c", "v1"))).isTrue();
		Assertions.assertThat(AdhocTranscodingHelper.match(inV1OrV2, Map.of("c", "v2"))).isTrue();
	}

	@Test
	public void testLike() {
		ColumnFilter startsWithV1 =
				ColumnFilter.builder().column("c").valueMatcher(LikeMatcher.builder().like("v1%").build()).build();

		Assertions.assertThat(AdhocTranscodingHelper.match(startsWithV1, Map.of())).isFalse();
		Assertions.assertThat(AdhocTranscodingHelper.match(startsWithV1, Map.of("c", "v3"))).isFalse();

		Assertions.assertThat(AdhocTranscodingHelper.match(startsWithV1, Map.of("c", "v1"))).isTrue();
		Assertions.assertThat(AdhocTranscodingHelper.match(startsWithV1, Map.of("c", "v2"))).isFalse();

		Assertions.assertThat(AdhocTranscodingHelper.match(startsWithV1, Map.of("c", "p_v1"))).isFalse();
		Assertions.assertThat(AdhocTranscodingHelper.match(startsWithV1, Map.of("c", "v1_s"))).isTrue();
	}

	@Test
	public void testIn_Transcoded() {
		IAdhocTableTranscoder transcoder = PrefixTranscoder.builder().prefix("p_").build();

		Assertions
				.assertThat(AdhocTranscodingHelper
						.match(transcoder, ColumnFilter.isIn("c", "c1", "c2"), Map.of("p_c", "c1")))
				.isTrue();

		Assertions.assertThat(AdhocTranscodingHelper.match(transcoder,
				AndFilter.and(ColumnFilter.isLike("c", "a%"), ColumnFilter.isLike("c", "%a")),
				Map.of("p_c", "a"))).isTrue();

		Assertions.assertThat(AdhocTranscodingHelper.match(transcoder,
				OrFilter.or(ColumnFilter.isLike("c", "a%"), ColumnFilter.isLike("c", "%a")),
				Map.of("p_c", "azerty"))).isTrue();
	}

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

		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of())).isTrue();
		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, mapOfMayBeNull("k", null))).isTrue();

		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of("k", "v"))).isFalse();
		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of("k", "v2"))).isFalse();

		Assertions.assertThat(kIsNull.toString()).isEqualTo("k matches `NullMatcher()` (nullIfAbsent=true)");
	}

	@Test
	public void testMatchNullButNotMissing() {
		ColumnFilter kIsNull = ColumnFilter.builder().column("k").matchNull().nullIfAbsent(false).build();

		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of())).isFalse();
		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, mapOfMayBeNull("k", null))).isTrue();

		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of("k", "v"))).isFalse();
		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of("k", "v2"))).isFalse();
	}

	@Test
	public void testMatchNotNull() {
		ColumnFilter filterToNegate = ColumnFilter.builder().column("k").matchNull().build();
		NotFilter filter = NotFilter.builder().negated(filterToNegate).build();

		Assertions.assertThat(AdhocTranscodingHelper.match(filter, Map.of())).isFalse();
		Assertions.assertThat(AdhocTranscodingHelper.match(filter, mapOfMayBeNull("k", null))).isFalse();

		Assertions.assertThat(AdhocTranscodingHelper.match(filter, Map.of("k", "v"))).isTrue();
		Assertions.assertThat(AdhocTranscodingHelper.match(filterToNegate, Map.of("k", "v2"))).isFalse();
	}

	@Test
	public void testJavaUtilSetOf() {
		ColumnFilter filter = ColumnFilter.builder().column("k").matching(Set.of("v1", "v2")).build();

		Assertions.assertThat(AdhocTranscodingHelper.match(filter, Map.of())).isFalse();
		Assertions.assertThat(AdhocTranscodingHelper.match(filter, mapOfMayBeNull("k", null))).isFalse();

		Assertions.assertThat(AdhocTranscodingHelper.match(filter, Map.of("k", "v2"))).isTrue();
		Assertions.assertThat(AdhocTranscodingHelper.match(filter, Map.of("k", "v3"))).isFalse();

		Assertions.assertThat(filter.toString()).isEqualTo("k matches `InMatcher{size=2, #0=v1, #1=v2}`");
	}

	@Test
	public void testCollectionsWithNull() {
		ColumnFilter filter = ColumnFilter.builder().column("k").matching(Arrays.asList(null, "v")).build();

		Assertions.assertThat(AdhocTranscodingHelper.match(filter, Map.of())).isTrue();
		Assertions.assertThat(AdhocTranscodingHelper.match(filter, mapOfMayBeNull("k", null))).isTrue();

		Assertions.assertThat(AdhocTranscodingHelper.match(filter, Map.of("k", "v"))).isTrue();
		Assertions.assertThat(AdhocTranscodingHelper.match(filter, Map.of("k", "v2"))).isFalse();
	}

	@Test
	public void testIsDistinctFrom() {
		ColumnFilter kIsNull = ColumnFilter.isDistinctFrom("k", "v");

		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of())).isTrue();
		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, mapOfMayBeNull("k", null))).isTrue();

		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of("k", "v"))).isFalse();
		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of("k", "v2"))).isTrue();
	}

	@Test
	public void testIsIn_single() {
		IAdhocFilter kIsNull = ColumnFilter.isIn("k", "v");

		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of())).isFalse();
		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, mapOfMayBeNull("k", null))).isFalse();

		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of("k", "v"))).isTrue();
		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of("k", "v2"))).isFalse();
	}

	@Test
	public void testIsIn_list() {
		IAdhocFilter kIsNull = ColumnFilter.isIn("k", List.of("v"));

		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of())).isFalse();
		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, mapOfMayBeNull("k", null))).isFalse();

		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of("k", "v"))).isTrue();
		Assertions.assertThat(AdhocTranscodingHelper.match(kIsNull, Map.of("k", "v2"))).isFalse();
	}
}
