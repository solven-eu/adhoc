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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.pepper.unittest.ILogDisabler;
import eu.solven.pepper.unittest.PepperTestHelper;

public class TestSumAggregation {
	SumAggregation aggregator = new SumAggregation();

	@Test
	public void testIsSum() {
		Assertions.assertThat(SumAggregation.isSum("SUM")).isTrue();
		Assertions.assertThat(SumAggregation.isSum("+")).isTrue();
		Assertions.assertThat(SumAggregation.isSum(SumAggregation.class.getName())).isTrue();

		Assertions.assertThat(SumAggregation.isSum("foo")).isFalse();
	}

	@Test
	public void testStrings() {
		Assertions.assertThat(aggregator.aggregate("someLongString_0", "someLongString_1"))
				.isEqualTo(List.of("someLongString_0", "someLongString_1"));
	}

	@Test
	public void testBigString() {
		List<Object> aggregated = List.of("initial");

		int size = 256;
		try (ILogDisabler logDisabler = PepperTestHelper.disableLog(SumAggregation.class)) {
			for (int i = 0; i < size; i++) {
				aggregated = (List) aggregator.aggregate(aggregated, "someLongString_" + i);
			}
		}

		Assertions.assertThat(aggregated)
				.startsWith("initial", "someLongString_0", "someLongString_1", "someLongString_2")
				.endsWith("someLongString_255")
				.hasSize(1 + size);
	}

	@Test
	public void testHugeString() {
		Assertions.assertThatThrownBy(() -> {
			List<Object> aggregated = List.of("initial");

			try (ILogDisabler logDisabler = PepperTestHelper.disableLog(SumAggregation.class)) {
				for (int i = 0; i < 16 * 1024; i++) {
					aggregated = (List) aggregator.aggregate(aggregated, "someLongString_" + i);
				}
			}
		}).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testIntAndInt() {
		Assertions.assertThat(aggregator.aggregate(123, 234)).isEqualTo(0L + 123 + 234);
	}

	@Test
	public void testIntAndLong() {
		Assertions.assertThat(aggregator.aggregate(123, 234L)).isEqualTo(0L + 123 + 234);
	}

	@Test
	public void testIntAndFloat() {
		Assertions.assertThat(aggregator.aggregate(123, 234.567D)).isEqualTo(0D + 123 + 234.567);
	}

	@Test
	public void testIntAndString() {
		Assertions.assertThat(aggregator.aggregate(123, "234")).isEqualTo(List.of(123, "234"));
	}

	@Test
	public void testSum_objects() {
		Assertions.assertThat(aggregator.aggregate((Object) null, null)).isEqualTo(null);
		Assertions.assertThat(aggregator.aggregate(null, 1.2D)).isEqualTo(1.2D);
		Assertions.assertThat(aggregator.aggregate(1.2D, null)).isEqualTo(1.2D);
	}

	@Test
	public void testSum_doubles() {
		Assertions.assertThat(aggregator.aggregateDoubles(Double.NaN, 1.2D)).isNaN();
		Assertions.assertThat(aggregator.aggregateDoubles(1.2D, Double.NaN)).isNaN();
		Assertions.assertThat(aggregator.aggregateDoubles(Double.NaN, Double.NaN)).isNaN();
	}

	@Test
	public void testSum_bigDecimal() {
		Assertions.assertThat(aggregator.aggregate(123, BigDecimal.valueOf(234L))).isEqualTo(0D + 123 + 234);
	}

	@Test
	public void testSum_bigInteger() {
		Assertions.assertThat(aggregator.aggregate(123, BigInteger.valueOf(234L))).isEqualTo(0L + 123 + 234);
	}

	@Test
	public void testSum_bigDecimal_singleLong() {
		Assertions.assertThat(aggregator.aggregate(null, BigDecimal.valueOf(234L))).isEqualTo(0D + 234L);
	}

	@Test
	public void testSum_bigDecimal_singleDouble() {
		Assertions.assertThat(aggregator.aggregate(null, BigDecimal.valueOf(12.34D))).isEqualTo(12.34D);
	}

	@Test
	public void testException() {
		RuntimeException e = new RuntimeException("someMessage");
		Assertions.assertThat(aggregator.aggregate(null, e)).isEqualTo(e);

		RuntimeException e2 = new IllegalArgumentException("someMessage");
		Assertions.assertThat(aggregator.aggregate(e2, e)).isEqualTo(e2);
	}
}
