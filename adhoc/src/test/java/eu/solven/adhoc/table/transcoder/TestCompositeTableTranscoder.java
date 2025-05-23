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
package eu.solven.adhoc.table.transcoder;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.table.transcoder.CompositeTableTranscoder.ChainMode;

public class TestCompositeTableTranscoder {
	@Test
	public void testFirstNotNull() {
		MapTableTranscoder transcoder1 =
				MapTableTranscoder.builder().queriedToUnderlying("a1", "b1").queriedToUnderlying("a2", "b2").build();
		MapTableTranscoder transcoder2 =
				MapTableTranscoder.builder().queriedToUnderlying("b1", "c1").queriedToUnderlying("b3", "c3").build();

		CompositeTableTranscoder transcoder = CompositeTableTranscoder.builder()
				.chainMode(ChainMode.FirstNotNull)
				.transcoder(transcoder1)
				.transcoder(transcoder2)
				.build();

		Assertions.assertThat(transcoder.underlying("a1")).isEqualTo("b1");
		Assertions.assertThat(transcoder.underlying("b1")).isEqualTo("c1");
		Assertions.assertThat(transcoder.underlying("a2")).isEqualTo("b2");
		Assertions.assertThat(transcoder.underlying("b3")).isEqualTo("c3");
		Assertions.assertThat(transcoder.underlying("unknown")).isEqualTo(null);
	}

	@Test
	public void testApplyTry() {
		MapTableTranscoder transcoder1 =
				MapTableTranscoder.builder().queriedToUnderlying("a1", "b1").queriedToUnderlying("a2", "b2").build();
		MapTableTranscoder transcoder2 =
				MapTableTranscoder.builder().queriedToUnderlying("b1", "c1").queriedToUnderlying("b3", "c3").build();

		CompositeTableTranscoder transcoder = CompositeTableTranscoder.builder()
				.chainMode(ChainMode.ApplyAll)
				.transcoder(transcoder1)
				.transcoder(transcoder2)
				.build();

		Assertions.assertThat(transcoder.underlying("a1")).isEqualTo("c1");
		Assertions.assertThat(transcoder.underlying("b1")).isEqualTo("c1");
		Assertions.assertThat(transcoder.underlying("a2")).isEqualTo("b2");
		Assertions.assertThat(transcoder.underlying("b3")).isEqualTo("c3");
		Assertions.assertThat(transcoder.underlying("unknown")).isEqualTo(null);
	}
}
