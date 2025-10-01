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
package eu.solven.adhoc.query.filter.optimizer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.NotFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.LikeMatcher;

public class TestFilterOptimizerHelpers {
	FilterOptimizerHelpers optimizer = FilterOptimizerHelpers.builder().build();

	AtomicBoolean hasSimplified = new AtomicBoolean();

	@Test
	public void testStripOr_orHasCommon() {
		Set<ISliceFilter> output = optimizer.splitThenStripOrs(hasSimplified,
				AndFilter.and(Map.of("a", "a1")),
				Set.of(OrFilter.or(Map.of("a", "a1", "b", "b2"))));

		Assertions.assertThat(output).hasSize(1).contains(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testStripOr() {
		Set<ISliceFilter> output = optimizer.splitThenStripOrs(hasSimplified,
				AndFilter.and(Map.of("a", "a1")),
				Set.of(OrFilter.or(Map.of("b", "b2", "c", "c3"))));

		Assertions.assertThat(output).hasSize(1).contains(OrFilter.or(Map.of("b", "b2", "c", "c3")));
	}

	@Test
	public void testStripOr_matchNone() {
		Set<ISliceFilter> output = optimizer.splitThenStripOrs(hasSimplified,
				AndFilter.and(Map.of("a", "a1")),
				Set.of(FilterBuilder
						.or(NotFilter.not(ColumnFilter.match("a", LikeMatcher.matching("a%"))),
								ColumnFilter.equalTo("b", "b2"))
						.combine()));

		Assertions.assertThat(output).hasSize(1).contains(OrFilter.or(Map.of("b", "b2")));
	}

	@Test
	public void testRemoverLaxerInAnd() {
		List<ISliceFilter> fromLaxToStrict =
				List.of(AndFilter.and(Map.of("a", "a1")), AndFilter.and(Map.of("a", "a1", "b", "b2")));

		Set<ISliceFilter> strippedAnd = optimizer.removeLaxerInAnd(fromLaxToStrict);
		Assertions.assertThat(strippedAnd).containsExactly(AndFilter.and(Map.of("a", "a1", "b", "b2")));

		Set<ISliceFilter> strippedOr = optimizer.removeStricterInOr(fromLaxToStrict);
		Assertions.assertThat(strippedOr).containsExactly(AndFilter.and(Map.of("a", "a1")));
	}

	@Test
	public void testRemoveLaxerInAnd_compareWithAll() {
		// a&b&(c|a&b) -> a&b
		List<ISliceFilter> fromLaxToStrict = List.of(AndFilter.and(Map.of("a", "a1")),
				AndFilter.and(Map.of("b", "b1")),
				FilterBuilder.or(AndFilter.and(Map.of("c", "c1")), AndFilter.and(Map.of("a", "a1", "b", "b1")))
						.combine());

		Set<ISliceFilter> strippedAnd = optimizer.removeLaxerInAnd(fromLaxToStrict);
		Assertions.assertThat(strippedAnd)
				.containsExactly(AndFilter.and(Map.of("a", "a1")), AndFilter.and(Map.of("b", "b1")));
	}

	@Disabled("TODO This test is false. The principle is interesting but we need first to craft a relevant case")
	@Test
	public void testRemoveStricterInOr_compareWithAll() {
		// a|b|(a|c)&(b|d) -> a|b
		List<ISliceFilter> fromLaxToStrict = List.of(AndFilter.and(Map.of("a", "a1")),
				AndFilter.and(Map.of("b", "b1")),
				FilterBuilder
						.and(OrFilter.or(ImmutableMap.of("a", "a1", "c", "c1")),
								OrFilter.or(ImmutableMap.of("b", "b1", "d", "d1")))
						.combine());

		Set<ISliceFilter> strippedOr = optimizer.removeStricterInOr(fromLaxToStrict);
		Assertions.assertThat(strippedOr)
				.containsExactly(AndFilter.and(Map.of("a", "a1")), AndFilter.and(Map.of("b", "b1")));
	}

	@Test
	public void testPack_InAndOutIsEmpty() {
		FilterOptimizerHelpers helper = FilterOptimizerHelpers.builder().build();
		ImmutableSet<? extends ISliceFilter> packed = helper.packColumnFilters(
				ImmutableSet.of(ColumnFilter.notEqualTo("d", "d1"), ColumnFilter.matchIn("d", "d1", "d2", "d3")));

		Assertions.assertThat((Set) packed).hasSize(1).containsExactly(ColumnFilter.matchIn("d", "d2", "d3"));
	}
}
