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

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

public class TestAdhocMapComparisonHelpers {
	@Test
	public void testCompareKeys_String() {
		Assertions.assertThat(AdhocMapComparisonHelpers.compareKey("a", "c")).isEqualTo(-2);
	}

	@Test
	public void testCompareKeys_KeySet() {
		Assertions.assertThat(AdhocMapComparisonHelpers.compareKeySet(ImmutableSet.of("a").iterator(),
				ImmutableSet.of("c").iterator())).isEqualTo(-2);

		Assertions.assertThat(AdhocMapComparisonHelpers.compareKeySet(ImmutableSet.of("a", "b").iterator(),
				ImmutableSet.of("c", "b").iterator())).isEqualTo(-2);

		Assertions.assertThat(AdhocMapComparisonHelpers.compareKeySet(ImmutableSet.of("a", "b").iterator(),
				ImmutableSet.of("a", "b").iterator())).isEqualTo(0);

		Assertions.assertThat(AdhocMapComparisonHelpers.compareKeySet(ImmutableSet.of("a", "b").iterator(),
				ImmutableSet.of("a", "c").iterator())).isEqualTo(-1);
	}

	@Test
	public void testCompareValues_List() {
		Assertions.assertThat(AdhocMapComparisonHelpers.compareValues(List.of("a"), List.of("c"))).isEqualTo(-2);
		Assertions.assertThat(AdhocMapComparisonHelpers.compareValues(List.of("b", "a"), List.of("b", "c")))
				.isEqualTo(-2);
		Assertions.assertThat(AdhocMapComparisonHelpers.compareValues(List.of("a", "b"), List.of("c", "b")))
				.isEqualTo(-2);
	}

	@Test
	public void testCompareValues_Iterators() {
		Assertions
				.assertThat(AdhocMapComparisonHelpers.compareValues2(List.of("a").iterator(), List.of("c").iterator()))
				.isEqualTo(-2);
		Assertions
				.assertThat(AdhocMapComparisonHelpers.compareValues2(List.of("b", "a").iterator(),
						List.of("b", "c").iterator()))
				.isEqualTo(-2);
		Assertions
				.assertThat(AdhocMapComparisonHelpers.compareValues2(List.of("a", "b").iterator(),
						List.of("c", "b").iterator()))
				.isEqualTo(-2);
	}
}
