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
package eu.solven.holymolap.measures.operator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSafeSumDoubleBinaryOperator {
	final SafeSumDoubleBinaryOperator operator = new SafeSumDoubleBinaryOperator();

	@Test
	public void testSimpleSum() {
		Assertions.assertThat(operator.applyAsDouble(123D, 456D)).isEqualTo(123D + 456D);
		Assertions.assertThat(operator.apply((Number) 123D, (Number) 456D)).isEqualTo(123D + 456D);
	}

	@Test
	public void testWithNaN() {
		Assertions.assertThat(operator.applyAsDouble(Double.NaN, 456D)).isEqualTo(456D);
		Assertions.assertThat(operator.applyAsDouble(123D, Double.NaN)).isEqualTo(123D);
		Assertions.assertThat(operator.applyAsDouble(Double.NaN, Double.NaN)).isEqualTo(0D);
	}
}