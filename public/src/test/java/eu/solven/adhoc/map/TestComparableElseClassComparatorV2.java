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

import java.util.Comparator;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.util.ComparableElseClassComparatorV2;

public class TestComparableElseClassComparatorV2 {
	Comparator<Object> comparator = new ComparableElseClassComparatorV2();

	@Test
	public void testCompare_null() {
		Assertions.assertThat(comparator.compare(1, null)).isEqualTo(-1);
		Assertions.assertThat(comparator.compare(null, "foo")).isEqualTo(1);
	}

	@Test
	public void testCompare_sameClass() {
		Assertions.assertThat(comparator.compare(1, 2)).isEqualTo(-1);
		Assertions.assertThat(comparator.compare("foo", "bar")).isEqualTo(4);

		Assertions.assertThat(comparator.compare("foo", 123)).isEqualTo(10);
		Assertions.assertThat(comparator.compare(123, "foo")).isEqualTo(-10);
	}

	@Test
	public void testCompare_notComparable() {
		Assertions.assertThat(comparator.compare(List.of("foo"), List.of("bar"))).isEqualTo(4);
		Assertions.assertThat(comparator.compare(List.of("bar"), List.of("foo"))).isEqualTo(-4);
	}
}
