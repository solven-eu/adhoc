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
package eu.solven.adhoc.sum;

import java.time.LocalDate;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.sum.AvgAggregation;
import eu.solven.adhoc.measure.sum.AvgAggregation.AvgCarrier;

public class TestAvgAggregation {
	AvgAggregation agg = new AvgAggregation();

	@Test
	public void testAggregation() {
		Assertions.assertThat(agg.aggregate((Object) null, null)).isEqualTo(null);

		Assertions.assertThat(agg.aggregate(123, null)).isEqualTo(AvgCarrier.of(123));
		Assertions.assertThat(agg.aggregate(null, 234)).isEqualTo(AvgCarrier.of(234));

		Assertions.assertThat(agg.aggregate(123, 234)).isInstanceOfSatisfying(AvgCarrier.class, c -> {
			Assertions.assertThat(c.getCount()).isEqualTo(2);
			Assertions.assertThat(c.getSumLong()).isEqualTo(0L + 123 + 234);
			Assertions.assertThat(c.getSumDouble()).isEqualTo(0D);
		});

		Assertions.assertThat(agg.aggregate(123, 23.45)).isInstanceOfSatisfying(AvgCarrier.class, c -> {
			Assertions.assertThat(c.getCount()).isEqualTo(2);
			Assertions.assertThat(c.getSumLong()).isEqualTo(0L + 123);
			Assertions.assertThat(c.getSumDouble()).isEqualTo(0D + 23.45);
		});
	}

	@Test
	public void testLocalDate() {
		LocalDate now = LocalDate.now();

		Assertions.assertThatThrownBy(() -> agg.aggregate(now, now))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Can not AVG around");

		Assertions.assertThatThrownBy(() -> agg.aggregate(null, "someString"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Can not AVG around");
	}

}
