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
package eu.solven.adhoc.table.transcoder;

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAdhocTranscodingHelper {
	@Test
	public void testSimpleMapping_Empty() {
		ITableTranscoder transcoder = MapTableTranscoder.builder().build();
		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("k")).isEqualTo("k");

		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of())).isEmpty();
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("k", "v")))
				.isEqualTo(Map.of("k", "v"));
		// Receiving unknown column would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("otherK", "v")))
				// .isEqualTo(Map.of("otherK", "v"))
				.isEmpty();
	}

	@Test
	public void testSimpleMapping() {
		ITableTranscoder transcoder = MapTableTranscoder.builder().queriedToUnderlying("k1", "k").build();
		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("k1")).isEqualTo("k");
		Assertions.assertThat(context.underlying("k2")).isEqualTo("k2");

		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of())).isEmpty();
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("k", "v")))
				.isEqualTo(Map.of("k1", "v"));
		// Receiving unknown column would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("otherK", "v")))
				// .isEqualTo(Map.of("otherK", "v"))
				.isEmpty();
	}

	@Test
	public void testSimpleMapping_RequestUnderlying() {
		ITableTranscoder transcoder = MapTableTranscoder.builder().queriedToUnderlying("k1", "k").build();
		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("k1")).isEqualTo("k");
		Assertions.assertThat(context.underlying("k")).isEqualTo("k");

		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of())).isEmpty();
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("k", "v")))
				.isEqualTo(Map.of("k1", "v", "k", "v"));
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("k1", "v")))
				// `k1` has not been requested
				// .isEqualTo(Map.of("k1", "v"))
				.isEmpty();

		// Receiving unknown column would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("otherK", "v")))
				// .isEqualTo(Map.of("otherK", "v"))
				.isEmpty();
	}

	@Test
	public void testOverlap() {
		ITableTranscoder transcoder =
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
				// .isEqualTo(Map.of("k1", "v"))
				.isEmpty();
		// Receiving unknown column would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcodeColumns(context, Map.of("otherK", "v")))
				// .isEqualTo(Map.of("otherK", "v"))
				.isEmpty();
	}

	@Test
	public void testSubOptimal_OK() {
		AdhocTranscodingHelper.COUNT_SUBOPTIMAL.set(0);

		Set<String> outputKeys = Set.of("k1", "k2");

		ITableReverseTranscoder transcoder = new ITableReverseTranscoder() {
			@Override
			public Set<String> queried(String underlying) {
				return outputKeys;
			}

			@Override
			public int estimateQueriedSize(Set<String> underlyingKeys) {
				return outputKeys.size();
			}
		};
		Map<String, ?> transcoded = AdhocTranscodingHelper.transcodeColumns(transcoder, Map.of("k", "v"));

		Assertions.assertThat((Map) transcoded).containsEntry("k1", "v").containsEntry("k2", "v").hasSize(2);
		Assertions.assertThat(AdhocTranscodingHelper.COUNT_SUBOPTIMAL).hasValue(0);
	}

	@Test
	public void testSubOptimal_TooLow() {
		AdhocTranscodingHelper.COUNT_SUBOPTIMAL.set(0);

		Set<String> outputKeys = Set.of("k1", "k2");

		ITableReverseTranscoder transcoder = new ITableReverseTranscoder() {
			@Override
			public Set<String> queried(String underlying) {
				return outputKeys;
			}

			@Override
			public int estimateQueriedSize(Set<String> underlyingKeys) {
				// `-1`: we are underestimating the actual number of entries to write
				return outputKeys.size() - 1;
			}
		};
		Map<String, ?> transcoded = AdhocTranscodingHelper.transcodeColumns(transcoder, Map.of("k", "v"));

		Assertions.assertThat((Map) transcoded).containsEntry("k1", "v").containsEntry("k2", "v").hasSize(2);
		Assertions.assertThat(AdhocTranscodingHelper.COUNT_SUBOPTIMAL).hasValue(1);
	}
}
