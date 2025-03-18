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
import java.time.LocalDate;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAdhocPrimitiveHelpers {

	@Test
	public void isLongLike() {
		Assertions.assertThat(AdhocPrimitiveHelpers.isLongLike(123)).isTrue();
		Assertions.assertThat(AdhocPrimitiveHelpers.isLongLike(123L)).isTrue();

		Assertions.assertThat(AdhocPrimitiveHelpers.isLongLike(123.4F)).isFalse();
		Assertions.assertThat(AdhocPrimitiveHelpers.isLongLike(123.4D)).isFalse();

		Assertions.assertThat(AdhocPrimitiveHelpers.isLongLike("someString")).isFalse();
		Assertions.assertThat(AdhocPrimitiveHelpers.isLongLike(LocalDate.now())).isFalse();

		// Float without decimal
		Assertions.assertThat(AdhocPrimitiveHelpers.isLongLike(123F)).isFalse();
		Assertions.assertThat(AdhocPrimitiveHelpers.isLongLike(123D)).isFalse();

		Assertions.assertThat(AdhocPrimitiveHelpers.isLongLike(BigDecimal.valueOf(123))).isTrue();
		Assertions.assertThat(AdhocPrimitiveHelpers.isLongLike(BigDecimal.valueOf(123.456))).isFalse();
	}

	@Test
	public void isDoubleLike() {
		Assertions.assertThat(AdhocPrimitiveHelpers.isDoubleLike(123)).isTrue();
		Assertions.assertThat(AdhocPrimitiveHelpers.isDoubleLike(123L)).isTrue();

		Assertions.assertThat(AdhocPrimitiveHelpers.isDoubleLike(123.4F)).isTrue();
		Assertions.assertThat(AdhocPrimitiveHelpers.isDoubleLike(123.4D)).isTrue();

		Assertions.assertThat(AdhocPrimitiveHelpers.isDoubleLike("someString")).isFalse();
		Assertions.assertThat(AdhocPrimitiveHelpers.isDoubleLike(LocalDate.now())).isFalse();

		// Float without decimal
		Assertions.assertThat(AdhocPrimitiveHelpers.isDoubleLike(123F)).isTrue();
		Assertions.assertThat(AdhocPrimitiveHelpers.isDoubleLike(123D)).isTrue();

		Assertions.assertThat(AdhocPrimitiveHelpers.isDoubleLike(BigDecimal.valueOf(123))).isTrue();
		Assertions.assertThat(AdhocPrimitiveHelpers.isDoubleLike(BigDecimal.valueOf(123.456))).isTrue();
	}
}
