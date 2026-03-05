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
package eu.solven.adhoc.primitive;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestValueProviderHelpers {

	@Test
	public void asLongIfExact_integer() {
		IValueProvider vp = ValueProviderHelpers.asLongIfExact(42);
		Assertions.assertThat(IValueProvider.getValue(vp)).isEqualTo(42L);
	}

	@Test
	public void asLongIfExact_negativeInteger() {
		IValueProvider vp = ValueProviderHelpers.asLongIfExact(-7);
		Assertions.assertThat(IValueProvider.getValue(vp)).isEqualTo(-7L);
	}

	@Test
	public void asLongIfExact_long() {
		IValueProvider vp = ValueProviderHelpers.asLongIfExact(Long.MAX_VALUE);
		Assertions.assertThat(IValueProvider.getValue(vp)).isEqualTo(Long.MAX_VALUE);
	}

	@Test
	public void asLongIfExact_long_negative() {
		IValueProvider vp = ValueProviderHelpers.asLongIfExact(Long.MIN_VALUE);
		Assertions.assertThat(IValueProvider.getValue(vp)).isEqualTo(Long.MIN_VALUE);
	}

	@Test
	public void asLongIfExact_bigInteger_fitsLong() {
		IValueProvider vp = ValueProviderHelpers.asLongIfExact(BigInteger.valueOf(12_345));
		Assertions.assertThat(IValueProvider.getValue(vp)).isEqualTo(12345L);
	}

	@Test
	public void asLongIfExact_bigInteger_tooLarge() {
		// Larger than Long.MAX_VALUE — cannot be represented as long, falls back to double
		BigInteger hugeValue = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
		IValueProvider vp = ValueProviderHelpers.asLongIfExact(hugeValue);
		Assertions.assertThat(IValueProvider.getValue(vp)).isInstanceOf(Double.class);
	}

	@Test
	public void asLongIfExact_bigDecimal_exactInteger() {
		IValueProvider vp = ValueProviderHelpers.asLongIfExact(BigDecimal.valueOf(456));
		Assertions.assertThat(IValueProvider.getValue(vp)).isEqualTo(456L);
	}

	@Test
	public void asLongIfExact_bigDecimal_fractional() {
		// Has decimal part — longValueExact() throws, falls back to double
		IValueProvider vp = ValueProviderHelpers.asLongIfExact(BigDecimal.valueOf(3.14));
		Assertions.assertThat(IValueProvider.getValue(vp)).isInstanceOf(Double.class);
	}

	@Test
	public void asLongIfExact_double() {
		// Double is not matched by any instanceof branch, falls to doubleValue()
		IValueProvider vp = ValueProviderHelpers.asLongIfExact(2.718D);
		Assertions.assertThat(IValueProvider.getValue(vp)).isEqualTo(2.718D);
	}

	@Test
	public void asLongIfExact_float() {
		// Float is not matched by any instanceof branch, falls to doubleValue()
		IValueProvider vp = ValueProviderHelpers.asLongIfExact(1.5F);
		Assertions.assertThat(IValueProvider.getValue(vp)).isEqualTo((double) 1.5F);
	}
}
