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
package eu.solven.adhoc.query.filter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;

public class TestAndFilter {
	// A short toString not to prevail is composition .toString
	@Test
	public void toString_grandTotal() {
		IAdhocFilter filter = IAdhocFilter.MATCH_ALL;

		Assertions.assertThat(filter.toString()).isEqualTo("matchAll");
		Assertions.assertThat(filter.isMatchAll()).isTrue();
	}

	@Test
	public void toString_small() {
		List<ColumnFilter> filters = IntStream.range(0, 5)
				.mapToObj(i -> ColumnFilter.builder().column("k" + i).matching(i).build())
				.collect(Collectors.toList());

		Assertions.assertThat(AndFilter.and(filters).toString()).isEqualTo("k0==0&k1==1&k2==2&k3==3&k4==4");
	}

	@Test
	public void toString_huge() {
		List<ColumnFilter> filters = IntStream.range(0, 256)
				.mapToObj(i -> ColumnFilter.builder().column("k" + i).matching(i).build())
				.collect(Collectors.toList());

		Assertions.assertThat(AndFilter.and(filters).toString())
				.contains("#0=k0==0", "#1=k1==1")
				.doesNotContain("7")
				.hasSizeLessThan(512);
	}

	@Test
	public void toString_andOr() {
		Assertions
				.assertThat(
						AndFilter
								.and(OrFilter.or(ImmutableMap.of("a", "a1", "b", "b1")),
										OrFilter.or(ImmutableMap.of("c", "c1", "d", "d1")))
								.toString())
				.isEqualTo("(a==a1|b==b1)&(c==c1|d==d1)");
	}

	@Test
	public void testAndFilters_twoGrandTotal() {
		IAdhocFilter filterAllAndA = AndFilter.and(IAdhocFilter.MATCH_ALL, IAdhocFilter.MATCH_ALL);

		Assertions.assertThat(filterAllAndA).isEqualTo(IAdhocFilter.MATCH_ALL);
	}

	@Test
	public void testAndFilters_oneGrandTotal() {
		IAdhocFilter filterAllAndA = AndFilter.and(IAdhocFilter.MATCH_ALL, ColumnFilter.isEqualTo("a", "a1"));

		Assertions.assertThat(filterAllAndA).isEqualTo(ColumnFilter.isEqualTo("a", "a1"));
	}

	@Test
	public void testAndFilters_oneGrandTotal_forced() {
		IAdhocFilter filterAllAndA =
				AndFilter.builder().filter(IAdhocFilter.MATCH_NONE).filter(ColumnFilter.isEqualTo("a", "a1")).build();

		// We forced an AndFilter: It is not simplified into IAdhocFilter.MATCH_NONE but is is isMatchNone
		Assertions.assertThat(filterAllAndA.isMatchNone()).isTrue();
		Assertions.assertThat(filterAllAndA).isNotEqualTo(IAdhocFilter.MATCH_NONE);
	}

	@Test
	public void testAndFilters_oneGrandTotal_TwoCustom() {
		IAdhocFilter filterAllAndA =
				AndFilter.and(IAdhocFilter.MATCH_ALL, ColumnFilter.isLike("a", "%a"), ColumnFilter.isLike("a", "a%"));

		Assertions.assertThat(filterAllAndA).isInstanceOfSatisfying(AndFilter.class, andF -> {
			Assertions.assertThat(andF.getOperands())
					.hasSize(2)
					.contains(ColumnFilter.isLike("a", "%a"), ColumnFilter.isLike("a", "a%"));
		});
	}

	@Test
	public void testMultipleSameColumn_equalsAndIn() {
		IAdhocFilter filterA1andInA12 =
				AndFilter.and(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isIn("a", "a1", "a2"));

		// At some point, this may be optimized into `ColumnFilter.isEqualTo("a", "a1")`
		Assertions.assertThat(filterA1andInA12).isEqualTo(filterA1andInA12);
	}

	@Test
	public void testMultipleSameColumn_InAndIn() {
		IAdhocFilter filterA1andInA12 =
				AndFilter.and(ColumnFilter.isIn("a", "a1", "a2"), ColumnFilter.isIn("a", "a2", "a3"));

		// At some point, this may be optimized into `ColumnFilter.isEqualTo("a", "a2")`
		Assertions.assertThat(filterA1andInA12).isEqualTo(filterA1andInA12);
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		IAdhocFilter filter = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b2"));

		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		String asString = objectMapper.writeValueAsString(filter);
		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				{
				  "type" : "and",
				  "filters" : [ {
				    "type" : "column",
				    "column" : "a",
				    "valueMatcher" : "a1",
				    "nullIfAbsent" : true
				  }, {
				    "type" : "column",
				    "column" : "b",
				    "valueMatcher" : "b2",
				    "nullIfAbsent" : true
				  } ]
				}
				""".trim());

		IAdhocFilter fromString = objectMapper.readValue(asString, IAdhocFilter.class);

		Assertions.assertThat(fromString).isEqualTo(filter);
	}

	@Test
	public void testJackson_empty() throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		IAdhocFilter fromString = objectMapper.readValue("{}", IAdhocFilter.class);

		Assertions.assertThat(fromString).isEqualTo(IAdhocFilter.MATCH_ALL);
	}

	@Test
	public void testChained() {
		IAdhocFilter a1Andb2 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b2"));
		IAdhocFilter a1Andb2AndC3 = AndFilter.and(a1Andb2, ColumnFilter.isEqualTo("c", "c3"));

		Assertions.assertThat(a1Andb2AndC3).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands()).hasSize(3);
		});
	}

	@Test
	public void testMultipleEqualsSameColumn_disjoint() {
		IAdhocFilter a1Anda2 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("a", "a2"));

		Assertions.assertThat(a1Anda2).isEqualTo(IAdhocFilter.MATCH_NONE);
	}

	@Test
	public void testMultipleEqualsSameColumn_joint() {
		IAdhocFilter a1Anda2 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("a", "a1"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.isEqualTo("a", "a1"));
	}

	@Test
	public void testMultipleEqualsSameColumn_deep() {
		IAdhocFilter a1Andb1 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b1"));

		IAdhocFilter a1Andb1AndB2 = AndFilter.and(a1Andb1, ColumnFilter.isEqualTo("b", "b2"));

		Assertions.assertThat(a1Andb1AndB2).isEqualTo(IAdhocFilter.MATCH_NONE);
	}

	@Test
	public void testMultipleEqualsSameColumn_joint_andComplexNotColumn() {
		IAdhocFilter a1Anda2 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"),
				ColumnFilter.isEqualTo("a", "a1"),
				OrFilter.or(ColumnFilter.isLike("a", "%a"), ColumnFilter.isLike("a", "a%")));

		Assertions.assertThat(a1Anda2)
				.isEqualTo(AndFilter.and(ColumnFilter.isEqualTo("a", "a1"),
						OrFilter.or(ColumnFilter.isLike("a", "%a"), ColumnFilter.isLike("a", "a%"))));
	}

	@Test
	public void testMultipleEqualsSameColumn_joint_andComplexColumn() {
		IAdhocFilter a1Anda2 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"),
				ColumnFilter.isEqualTo("a", "a1"),
				ColumnFilter.builder()
						.column("a")
						.valueMatcher(OrMatcher.builder()
								.operand(LikeMatcher.matching("%ab"))
								.operand(LikeMatcher.matching("a%"))
								.build())
						.build());

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.isEqualTo("a", "a1"));
	}

	@Test
	public void testMultipleEqualsSameColumn_joint_withOr() {
		IAdhocFilter a1Anda2 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"),
				ColumnFilter.isEqualTo("a", "a1"),
				OrFilter.or(ColumnFilter.isEqualTo("a", "a2"), ColumnFilter.isEqualTo("a", "a3")));

		Assertions.assertThat(a1Anda2)
				.isEqualTo(AndFilter.and(ColumnFilter.isEqualTo("a", "a1"),
						OrFilter.or(ColumnFilter.isEqualTo("a", "a2"), ColumnFilter.isEqualTo("a", "a3"))));
	}

	@Test
	public void testMultipleInSameColumn_disjoint() {
		IAdhocFilter a1Anda2 = AndFilter.and(ColumnFilter.isIn("a", "a1", "a2"), ColumnFilter.isIn("a", "a3", "a4"));

		Assertions.assertThat(a1Anda2).isEqualTo(IAdhocFilter.MATCH_NONE);
	}

	@Test
	public void testMultipleInSameColumn_joint_single() {
		IAdhocFilter a1Anda2 = AndFilter.and(ColumnFilter.isIn("a", "a1", "a2"), ColumnFilter.isIn("a", "a2", "a3"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.isEqualTo("a", "a2"));
	}

	@Test
	public void testMultipleInSameColumn_joint_multiple() {
		IAdhocFilter a1Anda2 =
				AndFilter.and(ColumnFilter.isIn("a", "a1", "a2", "a3"), ColumnFilter.isIn("a", "a2", "a3", "a4"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.isIn("a", "a2", "a3"));
	}

	@Test
	public void testEquals_differentOrders() {
		IAdhocFilter f1Then2 = AndFilter.and(ColumnFilter.isEqualTo("c1", "v1"), ColumnFilter.isEqualTo("c2", "v2"));
		IAdhocFilter f2Then1 = AndFilter.and(ColumnFilter.isEqualTo("c2", "v2"), ColumnFilter.isEqualTo("c1", "v1"));

		Assertions.assertThat(f1Then2).isEqualTo(f2Then1);
	}

	@Test
	public void testFromMap() {
		// size==0
		Assertions.assertThat(AndFilter.and(Map.of())).isEqualTo(IAdhocFilter.MATCH_ALL);
		// size==1
		Assertions.assertThat(AndFilter.and(Map.of("c1", "v1"))).isEqualTo(ColumnFilter.isEqualTo("c1", "v1"));
		// size==2
		Assertions.assertThat(AndFilter.and(Map.of("c1", "v1", "c2", "v2")))
				.isEqualTo(AndFilter.and(ColumnFilter.isEqualTo("c1", "v1"), ColumnFilter.isEqualTo("c2", "v2")));
	}

	@Test
	public void testAnd_allNotFilter() {
		IAdhocFilter notA1AndNotA2 = AndFilter.and(NotFilter.not(ColumnFilter.isIn("a", "a1", "a2")),
				NotFilter.not(ColumnFilter.isIn("b", "b1", "b2")));

		// TODO Should we simplify the not?
		// Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(NotFilter.class, notFilter -> {
		// Assertions.assertThat(notFilter.getNegated())
		// .isEqualTo(NotFilter
		// .not(OrFilter.or(ColumnFilter.isIn("a", "a1", "a2"), ColumnFilter.isIn("b", "b1", "b2"))));
		// });

		Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands())
					.contains(NotFilter.not(ColumnFilter.isIn("a", "a1", "a2")),
							NotFilter.not(ColumnFilter.isIn("b", "b1", "b2")));
		});
	}

	@Test
	public void testAnd_allNotFilter_like_2() {
		List<IAdhocFilter> nots = Arrays.asList(NotFilter.not(ColumnFilter.isLike("a", "a%")),
				NotFilter.not(ColumnFilter.isLike("b", "b%")));
		IAdhocFilter notA1AndNotA2 = AndFilter.and(nots);

		Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands()).containsAll(nots);
		});
	}

	@Test
	public void testAnd_allNotFilter_like_3() {
		List<IAdhocFilter> likes =
				List.of(ColumnFilter.isLike("a", "a%"), ColumnFilter.isLike("b", "b%"), ColumnFilter.isLike("c", "c%"));
		List<IAdhocFilter> nots = likes.stream().map(NotFilter::not).toList();

		// And over 3 Not
		Assertions.assertThat(AndFilter.costFunction(AndFilter.builder().filters(nots).build())).isEqualTo(3 + 3 + 3);

		// Not over Or over 3 simple: cost==8
		Assertions
				.assertThat(AndFilter
						.costFunction(NotFilter.builder().negated(OrFilter.builder().filters(likes).build()).build()))
				.isEqualTo(3 + 2 + 1 + 1 + 1);

		IAdhocFilter notA1AndNotA2 = AndFilter.and(nots);

		Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(NotFilter.class, notFilter -> {
			// cost==5, which is cheaper than 8
			Assertions.assertThat(AndFilter.costFunction(notFilter)).isEqualTo(3 + 2 + 1 + 1 + 1);

			Assertions.assertThat(notFilter.getNegated()).isInstanceOfSatisfying(OrFilter.class, orFilter -> {
				Assertions.assertThat(orFilter.getOperands()).containsAll(likes);
			});
		});
	}

	@Test
	public void testAnd_allNotMatcher() {
		IAdhocFilter notA1AndNotB1 =
				AndFilter.and(ColumnFilter.isDistinctFrom("a", "a1"), ColumnFilter.isDistinctFrom("b", "b1"));

		// TODO Should we simplify the nots?
		// Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(NotFilter.class, notFilter -> {
		// Assertions.assertThat(notFilter.getNegated())
		// .isEqualTo(NotFilter
		// .not(OrFilter.or(ColumnFilter.isIn("a", "a1", "a2"), ColumnFilter.isIn("b", "b1", "b2"))));
		// });

		Assertions.assertThat(notA1AndNotB1).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands())
					.contains(ColumnFilter.isDistinctFrom("a", "a1"), ColumnFilter.isDistinctFrom("b", "b1"));
		});
	}

	@Test
	public void testAnd_partialNot() {
		IAdhocFilter notA1AndNotA2 =
				AndFilter.and(NotFilter.not(ColumnFilter.isIn("a", "a1", "a2")), ColumnFilter.isIn("b", "b1", "b2"));
		Assertions.assertThat(notA1AndNotA2).isInstanceOf(AndFilter.class);
		Assertions.assertThat(AndFilter.and(notA1AndNotA2, notA1AndNotA2)).isInstanceOf(AndFilter.class);
	}

	@Test
	public void testAnd_orDifferentColumns_sameColumn() {
		ColumnFilter left = ColumnFilter.isEqualTo("g", "c1");
		IAdhocFilter leftOrRight = OrFilter.or(left, ColumnFilter.isEqualTo("h", "c1"));

		Assertions.assertThat(AndFilter.and(leftOrRight, left)).isEqualTo(left);
	}

	@Test
	public void testCostFunction() {
		Assertions.assertThat(AndFilter.costFunction(Set.of(OrFilter.or(Map.of("a", "a1"))))).isEqualTo(1);
		Assertions.assertThat(AndFilter.costFunction(Set.of(AndFilter.and(Map.of("a", "a1"))))).isEqualTo(1);

		Assertions
				.assertThat(OrFilter.or(AndFilter.and(ImmutableMap.of("a", "a1")),
						AndFilter.and(ImmutableMap.of("b", "b1", "c", "c1"))))
				.hasToString("a==a1|b==b1&c==c1")
				.satisfies(f -> {
					Assertions.assertThat(AndFilter.costFunction(f)).isEqualTo(1 + 2 + 1 + 1);
				});

		Assertions
				.assertThat(AndFilter.and(AndFilter.and(ImmutableMap.of("a", "a1")),
						OrFilter.or(ImmutableMap.of("b", "b1", "c", "c1"))))
				.hasToString("a==a1&(b==b1|c==c1)")
				.satisfies(f -> {
					Assertions.assertThat(AndFilter.costFunction(f)).isEqualTo(1 + 1 + 2 + 1);
				});

		Assertions
				.assertThat(AndFilter.and(AndFilter.and(ImmutableMap.of("a", "a1")),
						OrFilter.or(NotFilter.not(ColumnFilter.isLike("b", "b%")), ColumnFilter.isLike("c", "c%"))))
				.hasToString("a==a1&(b does NOT match `LikeMatcher(pattern=b%)`|c matches `LikeMatcher(pattern=c%)`)")
				.satisfies(f -> {
					Assertions.assertThat(AndFilter.costFunction(f)).isEqualTo(1 + 3 + 2 + 1);
				});
	}
}
