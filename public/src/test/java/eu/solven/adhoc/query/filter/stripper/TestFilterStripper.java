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
package eu.solven.adhoc.query.filter.stripper;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.OrFilter;

public class TestFilterStripper {
	@Test
	public void testSharedCache() {
		FilterStripper stripper = FilterStripper.builder().where(AndFilter.and(Map.of("c", "c1", "d", "d2"))).build();
		Assertions.assertThat(stripper.filterToStripper.asMap()).isEmpty();

		Assertions.assertThat(stripper.isStricterThan(ColumnFilter.matchEq("c", "c1"))).isTrue();
		Assertions.assertThat(stripper.filterToStripper.asMap()).hasSize(1);

		FilterStripper relatedStripper = stripper.withWhere(ColumnFilter.matchEq("e", "e3"));
		// Ensure the cache unrelated to current WHERE is shared
		Assertions.assertThat(relatedStripper.filterToStripper.asMap()).hasSize(1);
		// Ensure the cache related to current WHERE is not-shared
		Assertions.assertThat(relatedStripper.knownAsStricter.asMap()).isEmpty();
	}

	@Test
	public void testOrMatchAll() {
		FilterStripper stripper = FilterStripper.builder().where(AndFilter.and(Map.of("a", "a1"))).build();

		ISliceFilter laxerWithOr = OrFilter.or(Map.of("a", "a1", "b", "b1"));
		Assertions.assertThat(stripper.isStricterThan(laxerWithOr)).isTrue();
		Assertions.assertThat(stripper.strip(laxerWithOr)).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void test_notIn_and() {
		FilterStripper stripper = FilterStripper.builder().where(ColumnFilter.notIn("a", "a1", "a2", "a3")).build();

		ISliceFilter notInLessColumns =
				FilterBuilder.and(ColumnFilter.notIn("a", "a1", "a2"), ColumnFilter.notEq("b", "b1")).combine();
		Assertions.assertThat(stripper.isStricterThan(notInLessColumns)).isFalse();
		Assertions.assertThat(stripper.strip(notInLessColumns)).isEqualTo(ColumnFilter.notEq("b", "b1"));
	}

	@Test
	public void test_notIn_or() {
		FilterStripper stripper = FilterStripper.builder().where(ColumnFilter.notIn("a", "a1", "a2", "a3")).build();

		ISliceFilter notInLessColumns =
				FilterBuilder.or(ColumnFilter.notIn("a", "a1", "a2"), ColumnFilter.notEq("b", "b1")).combine();
		Assertions.assertThat(stripper.isStricterThan(notInLessColumns)).isTrue();
		Assertions.assertThat(stripper.strip(notInLessColumns)).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	// This is an example where `isStrictedThan` is false, but the output of `.strip` is MATCH_NONE
	@Test
	public void test_notIn_or_additionalColumn() {
		FilterStripper stripper = FilterStripper.builder()
				.where(FilterBuilder.and(ColumnFilter.notIn("a", "a1", "a2", "a3"), ColumnFilter.notEq("c", "c1"))
						.combine())
				.build();

		ISliceFilter notInLessColumns =
				FilterBuilder.or(ColumnFilter.notIn("a", "a1", "a2"), ColumnFilter.notEq("b", "b1")).combine();
		// Not stricter as input does not match `c!=c1`
		Assertions.assertThat(stripper.isStricterThan(notInLessColumns)).isFalse();
		// Still, `.strip` is everything is the constrain on `a` reject all rows
		Assertions.assertThat(stripper.strip(notInLessColumns)).isEqualTo(ISliceFilter.MATCH_ALL);
	}
}
