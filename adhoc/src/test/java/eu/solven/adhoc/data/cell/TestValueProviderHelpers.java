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
package eu.solven.adhoc.data.cell;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.column.IValueProviderTestHelpers;

public class TestValueProviderHelpers {
	@Test
	public void testLongExact() {
		Assertions.assertThat(IValueProviderTestHelpers.getLong(ValueProviderHelpers.asLongIfExact(123)))
				.isEqualTo(123L);
		Assertions.assertThat(IValueProviderTestHelpers.getLong(ValueProviderHelpers.asLongIfExact(123L)))
				.isEqualTo(123L);
		Assertions.assertThat(IValueProviderTestHelpers.getDouble(ValueProviderHelpers.asLongIfExact(123D)))
				.isEqualTo(123D);

		Assertions
				.assertThat(IValueProviderTestHelpers
						.getLong(ValueProviderHelpers.asLongIfExact(BigInteger.valueOf(Long.MAX_VALUE))))
				.isEqualTo(Long.MAX_VALUE);
		Assertions
				.assertThat(IValueProviderTestHelpers.getDouble(ValueProviderHelpers
						.asLongIfExact(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2)))))
				.isCloseTo(Double.parseDouble("1.8446744E19"), Percentage.withPercentage(0.0001));

		Assertions
				.assertThat(IValueProviderTestHelpers
						.getDouble(ValueProviderHelpers.asLongIfExact(BigDecimal.valueOf(Double.MAX_VALUE))))
				.isCloseTo(Double.parseDouble("1.797693E308"), Percentage.withPercentage(0.0001));
	}
}
