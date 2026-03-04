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
package eu.solven.adhoc.map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueProviderTestHelpers;
import eu.solven.adhoc.query.filter.value.NullMatcher;

public class TestStandardCoordinateNormalizer {
	StandardCoordinateNormalizer normalizer = new StandardCoordinateNormalizer();

	@Test
	public void test_Int() {
		Assertions.assertThat(normalizer.normalizeCoordinate(123)).isEqualTo(123L);

		Assertions
				.assertThat(
						IValueProviderTestHelpers.getLong(normalizer.normalizeCoordinate(IValueProvider.setValue(123))))
				.isEqualTo(123L);
	}

	@Test
	public void test_Float() {
		Assertions.assertThat(normalizer.normalizeCoordinate(12.34)).isEqualTo(12.34D);

		Assertions
				.assertThat(IValueProviderTestHelpers
						.getDouble(normalizer.normalizeCoordinate(IValueProvider.setValue(12.34))))
				.isEqualTo(12.34D);
	}

	@Test
	public void test_NULL() {
		Assertions.assertThat(normalizer.normalizeCoordinate((Object) null)).isEqualTo(NullMatcher.NULL_HOLDER);
		Assertions.assertThat(normalizer.normalizeCoordinate(NullMatcher.NULL_HOLDER))
				.isEqualTo(NullMatcher.NULL_HOLDER);

		Assertions
				.assertThat(IValueProviderTestHelpers
						.getObject(normalizer.normalizeCoordinate(IValueProvider.setValue(null))))
				.isEqualTo(NullMatcher.NULL_HOLDER);

		Assertions.assertThatThrownBy(() -> normalizer.normalizeCoordinate((IValueProvider) null))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
