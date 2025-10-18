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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFilterBuilder {
	@Test
	public void testAnd_combine_filterMatchAll() {
		Assertions.assertThat(FilterBuilder.and().combine()).isEqualTo(ISliceFilter.MATCH_ALL);

		Assertions.assertThat(FilterBuilder.and(ISliceFilter.MATCH_ALL, ColumnFilter.matchEq("c", "v")).combine())
				.isEqualTo(ColumnFilter.matchEq("c", "v"));
	}

	@Test
	public void testAnd_combine_filterMatchNone() {
		Assertions.assertThat(FilterBuilder.and(ISliceFilter.MATCH_NONE, ColumnFilter.matchEq("c", "v")).combine())
				.isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testOr_combine_filterMatchNone() {
		Assertions.assertThat(FilterBuilder.or().combine()).isEqualTo(ISliceFilter.MATCH_NONE);

		Assertions.assertThat(FilterBuilder.or(ISliceFilter.MATCH_NONE, ColumnFilter.matchEq("c", "v")).combine())
				.isEqualTo(ColumnFilter.matchEq("c", "v"));
	}

	@Test
	public void testOr_combine_filterMatchAll() {
		Assertions.assertThat(FilterBuilder.or(ISliceFilter.MATCH_ALL, ColumnFilter.matchEq("c", "v")).combine())
				.isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testOr_combine_forced() {
		// We forced an OrBuilder: It is simplified into IAdhocFilter.MATCH_ALL but is is a trivial isMatchAll
		{
			ISliceFilter builderCombine =
					FilterBuilder.or(ISliceFilter.MATCH_ALL, ColumnFilter.matchEq("a", "a1")).combine();

			Assertions.assertThat(builderCombine.isMatchAll()).isTrue();
			Assertions.assertThat(builderCombine).isEqualTo(builderCombine);
		}

		// `OrFilter.builder()` has absolutely no simplification rule
		{
			ISliceFilter manual =
					OrFilter.builder().or(ISliceFilter.MATCH_ALL).or(ColumnFilter.matchEq("a", "a1")).build();

			Assertions.assertThat(manual.isMatchAll()).isTrue();
			Assertions.assertThat(manual).isNotEqualTo(ISliceFilter.MATCH_ALL);
		}
	}

}
