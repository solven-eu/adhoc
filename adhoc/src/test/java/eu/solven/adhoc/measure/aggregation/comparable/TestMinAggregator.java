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
package eu.solven.adhoc.measure.aggregation.comparable;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMinAggregator {
	MinAggregation a = MinAggregation.builder().build();

	@Test
	public void testNull() {
		Assertions.assertThat(a.aggregate((Object) null, null)).isNull();
		Assertions.assertThat(a.aggregate(null, 123)).isEqualTo(123);
		Assertions.assertThat(a.aggregate(123, null)).isEqualTo(123);

		Assertions.assertThat(a.aggregate(null, "someS")).isEqualTo("someS");
		Assertions.assertThat(a.aggregate("someS", null)).isEqualTo("someS");
	}

	@Test
	public void testNominal() {
		// TODO Is it legal to cast to long?
		Assertions.assertThat(a.aggregate(123, 234)).isEqualTo(123L);

		Assertions.assertThat(a.aggregate("qwerty", "azerty")).isEqualTo("azerty");
		// Java considers lowerCase greater than upperCase
		Assertions.assertThat(a.aggregate("a", "A")).isEqualTo("A");
		Assertions.assertThat(a.aggregate("A", "a")).isEqualTo("A");
	}

	@Test
	public void testDifferentTypes() {
		Assertions.assertThat(a.aggregate(123, "a")).isEqualTo(123);
	}
}
