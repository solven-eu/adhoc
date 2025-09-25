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
package eu.solven.adhoc.query.filter;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFilterEqualsHelpers {
	@Test
	public void testEquivalent_inEquivalentOr() {
		ISliceFilter in = ColumnFilter.matchIn("c", "c1", "c2");
		ISliceFilter or =
				OrFilter.builder().or(ColumnFilter.equalTo("c", "c1")).or(ColumnFilter.equalTo("c", "c2")).build();

		Assertions.assertThat(in).isNotEqualTo(or);
		Assertions.assertThat(FilterEquivalencyHelpers.areEquivalent(in, or)).isTrue();
	}

	@Test
	public void testEquivalent_andOrEquivalentListedOr() {
		ISliceFilter inA = ColumnFilter.matchIn("a", "a1", "a2");
		ISliceFilter inB = ColumnFilter.matchIn("b", "b1", "b2");
		ISliceFilter and = AndFilter.builder().and(inA).and(inB).build();
		ISliceFilter or = OrFilter.builder()
				.or(AndFilter.and(Map.of("a", "a1", "b", "b1")))
				.or(AndFilter.and(Map.of("a", "a1", "b", "b2")))
				.or(AndFilter.and(Map.of("a", "a2", "b", "b1")))
				.or(AndFilter.and(Map.of("a", "a2", "b", "b2")))
				.build();

		Assertions.assertThat(and).isNotEqualTo(or);
		Assertions.assertThat(FilterEquivalencyHelpers.areEquivalent(and, or)).isTrue();
	}
}
