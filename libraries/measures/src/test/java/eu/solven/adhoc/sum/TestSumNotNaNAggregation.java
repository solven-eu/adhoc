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
package eu.solven.adhoc.sum;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.sum.SumNotNaNAggregation;

public class TestSumNotNaNAggregation {
	SumNotNaNAggregation a = new SumNotNaNAggregation();

	@Test
	public void testSum_objects() {
		Assertions.assertThat(a.aggregate((Object) null, null)).isEqualTo(null);
		Assertions.assertThat(a.aggregate(null, 1.2D)).isEqualTo(1.2D);
		Assertions.assertThat(a.aggregate(1.2D, null)).isEqualTo(1.2D);

		Assertions.assertThat(a.aggregate(123, 234)).isEqualTo(0L + 123 + 234);
	}

	@Test
	public void testSum_doubles() {
		Assertions.assertThat(a.aggregateDoubles(Double.NaN, 1.2D)).isEqualTo(1.2D);
		Assertions.assertThat(a.aggregateDoubles(1.2D, Double.NaN)).isEqualTo(1.2D);
		Assertions.assertThat(a.aggregateDoubles(Double.NaN, Double.NaN)).isEqualTo(0D);

		Assertions.assertThat(a.aggregateDoubles(1.2D, 2.3D)).isEqualTo(1.2D + 2.3D);
	}

	@Test
	public void testSum_long() {
		Assertions.assertThat(a.aggregateLongs(123, 234)).isEqualTo(0L + 123 + 234);
	}

	// TODO This is probably not satisfactory to overflow
	@Test
	public void testSum_long_overflow() {
		Assertions.assertThat(a.aggregateLongs(Long.MAX_VALUE, 234)).isEqualTo(0L + Long.MAX_VALUE + 234);
	}
}
