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

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.query.filter.optimizer.StandardFilterCostFunction;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;

public class TestStandardCostFunction {
	StandardFilterCostFunction costFunction = new StandardFilterCostFunction();

	@Test
	public void testCostFunction() {
		Assertions.assertThat(costFunction.cost(ColumnFilter.matchEq("a", "a1"))).isEqualTo(3);

		Assertions
				.assertThat(
						FilterBuilder
								.or(AndFilter.and(ImmutableMap.of("a", "a1")),
										AndFilter.and(ImmutableMap.of("b", "b1", "c", "c1")))
								.optimize())
				.hasToString("a==a1|b==b1&c==c1")
				.satisfies(f -> {
					Assertions.assertThat(costFunction.cost(f)).isEqualTo(3 + 5 + 3 + 3);
				});

		Assertions
				.assertThat(AndFilter.and(AndFilter.and(ImmutableMap.of("a", "a1")),
						OrFilter.or(ImmutableMap.of("b", "b1", "c", "c1"))))
				.hasToString("a==a1&(b==b1|c==c1)")
				.satisfies(f -> {
					Assertions.assertThat(costFunction.cost(f)).isEqualTo(3 + 5 + 3 + 3);
				});

		Assertions
				.assertThat(AndFilter.and(AndFilter.and(ImmutableMap.of("a", "a1")),
						FilterBuilder.or(ColumnFilter.matchLike("b", "b%").negate(), ColumnFilter.matchLike("c", "c%"))
								.optimize()))
				// .hasToString("a==a1&(b does NOT match `LikeMatcher(pattern=b%)`|c matches
				// `LikeMatcher(pattern=c%)`)")
				.hasToString("a==a1&(b does NOT match `LikeMatcher(pattern=b%)`|c matches `LikeMatcher(pattern=c%)`)")
				.satisfies(f -> {
					Assertions.assertThat(costFunction.cost(f)).isEqualTo(3 + 5 + 10 + 3);
				});
	}

	@Test
	public void testCostFunction_notOfNot() {
		// `a==a1`
		Assertions.assertThat(costFunction.cost(ColumnFilter.match("c", EqualsMatcher.matchEq(123L)))).isEqualTo(3);

		// `a!=a1`
		Assertions
				.assertThat(costFunction.cost(
						ColumnFilter.match("c", NotMatcher.builder().negated(EqualsMatcher.matchEq(123L)).build())))
				.isEqualTo(5);

		// `!(a==a1)`
		Assertions
				.assertThat(costFunction.cost(
						NotFilter.builder().negated(ColumnFilter.match("c", EqualsMatcher.matchEq(123L))).build()))
				.isEqualTo(2 * 3);

		// `not(not(a==a1))`
		Assertions.assertThat(costFunction.cost(ColumnFilter.match("c",
				NotMatcher.builder()
						.negated(NotMatcher.builder().negated(EqualsMatcher.matchEq(123L)).build())
						.build())))
				.isEqualTo(2 * 2 * 3);
	}

	@Test
	public void testAnd_allNotFilter_like_3() {
		List<ISliceFilter> likes = List.of(ColumnFilter.matchLike("a", "a%"),
				ColumnFilter.matchLike("b", "b%"),
				ColumnFilter.matchLike("c", "c%"));
		List<ISliceFilter> nots = likes.stream().map(ISliceFilter::negate).toList();

		// And over 3 Not
		Assertions.assertThat(costFunction.cost(AndFilter.builder().ands(nots).build())).isEqualTo(10 + 10 + 10);

		// Not over Or over 3 simple
		Assertions
				.assertThat(
						costFunction.cost(NotFilter.builder().negated(OrFilter.builder().ors(likes).build()).build()))
				.isEqualTo(2 * (5 + (3 + 3 + 3)));

		ISliceFilter notA1AndNotA2 = FilterBuilder.and(nots).optimize();

		Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(NotFilter.class, notFilter -> {
			// cost==7, which is cheaper than 8
			Assertions.assertThat(costFunction.cost(notFilter)).isEqualTo(2 * (5 + 3 + 3 + 3));

			Assertions.assertThat(notFilter.getNegated()).isInstanceOfSatisfying(OrFilter.class, orFilter -> {
				Assertions.assertThat(orFilter.getOperands()).containsAll(likes);
			});
		});
	}

}
