/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.dataframe.column.navigable;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBloomKeyPresencePreScreen {

	@Test
	public void testEmpty_mightContainReturnsFalse() {
		BloomKeyPresencePreScreen<String> filter = new BloomKeyPresencePreScreen<>(64, 0.01);
		Assertions.assertThat(filter.mightContain("a")).isFalse();
	}

	@Test
	public void testAddedKey_mightContainReturnsTrue() {
		BloomKeyPresencePreScreen<String> filter = new BloomKeyPresencePreScreen<>(64, 0.01);

		filter.add("a");
		Assertions.assertThat(filter.mightContain("a")).isTrue();

		filter.add("b");
		Assertions.assertThat(filter.mightContain("a")).isTrue();
		Assertions.assertThat(filter.mightContain("b")).isTrue();
	}

	@Test
	public void testManyKeys_noFalseNegatives() {
		BloomKeyPresencePreScreen<String> filter = new BloomKeyPresencePreScreen<>(1000, 0.01);

		for (int i = 0; i < 1000; i++) {
			filter.add("k" + i);
		}

		// Bloom contract: every added key MUST be reported as mightContain == true.
		for (int i = 0; i < 1000; i++) {
			Assertions.assertThat(filter.mightContain("k" + i)).as("key k%d", i).isTrue();
		}
	}

	@Test
	public void testCustomFunnel() {
		// Use the default funnel via the public-funnel constructor.
		BloomKeyPresencePreScreen<String> filter =
				new BloomKeyPresencePreScreen<>(BloomKeyPresencePreScreen.DefaultFunnel.INSTANCE, 64, 0.01);

		filter.add("hello");
		Assertions.assertThat(filter.mightContain("hello")).isTrue();
		Assertions.assertThat(filter.mightContain("notpresent")).isFalse();
	}
}
