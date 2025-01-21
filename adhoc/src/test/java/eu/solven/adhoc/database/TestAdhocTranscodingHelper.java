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
package eu.solven.adhoc.database;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.database.transcoder.AdhocTranscodingHelper;
import eu.solven.adhoc.database.transcoder.IAdhocDatabaseTranscoder;
import eu.solven.adhoc.database.transcoder.MapDatabaseTranscoder;
import eu.solven.adhoc.database.transcoder.TranscodingContext;

public class TestAdhocTranscodingHelper {
	@Test
	public void testSimpleMapping_Empty() {
		IAdhocDatabaseTranscoder transcoder = MapDatabaseTranscoder.builder().build();
		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("k")).isEqualTo("k");

		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of())).isEmpty();
		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of("k", "v"))).isEqualTo(Map.of("k", "v"));
		// Receiving unknown column would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of("otherK", "v")))
				.isEqualTo(Map.of("otherK", "v"));
	}

	@Test
	public void testSimpleMapping() {
		IAdhocDatabaseTranscoder transcoder = MapDatabaseTranscoder.builder().queriedToUnderlying("k1", "k").build();
		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("k1")).isEqualTo("k");
		Assertions.assertThat(context.underlying("k2")).isEqualTo("k2");

		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of())).isEmpty();
		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of("k", "v"))).isEqualTo(Map.of("k1", "v"));
		// Receiving unknown column would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of("otherK", "v")))
				.isEqualTo(Map.of("otherK", "v"));
	}

	@Test
	public void testSimpleMapping_RequestUnderlying() {
		IAdhocDatabaseTranscoder transcoder = MapDatabaseTranscoder.builder().queriedToUnderlying("k1", "k").build();
		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("k1")).isEqualTo("k");
		Assertions.assertThat(context.underlying("k")).isEqualTo("k");

		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of())).isEmpty();
		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of("k", "v")))
				.isEqualTo(Map.of("k1", "v", "k", "v"));
		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of("k1", "v")))
				.isEqualTo(Map.of("k1", "v"));
		// Receiving unknown column would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of("otherK", "v")))
				.isEqualTo(Map.of("otherK", "v"));
	}

	@Test
	public void testOverlap() {
		IAdhocDatabaseTranscoder transcoder =
				MapDatabaseTranscoder.builder().queriedToUnderlying("k1", "k2").queriedToUnderlying("k2", "k3").build();
		TranscodingContext context = TranscodingContext.builder().transcoder(transcoder).build();

		Assertions.assertThat(context.underlying("k1")).isEqualTo("k2");
		Assertions.assertThat(context.underlying("k2")).isEqualTo("k3");

		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of())).isEmpty();
		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of("k2", "v")))
				.isEqualTo(Map.of("k1", "v"));
		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of("k3", "v")))
				.isEqualTo(Map.of("k2", "v"));

		// Receiving k1 from DB would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of("k1", "v")))
				.isEqualTo(Map.of("k1", "v"));
		// Receiving unknown column would be a bug
		Assertions.assertThat(AdhocTranscodingHelper.transcode(context, Map.of("otherK", "v")))
				.isEqualTo(Map.of("otherK", "v"));
	}
}
