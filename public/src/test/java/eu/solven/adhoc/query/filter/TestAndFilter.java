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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import eu.solven.adhoc.resource.AdhocPublicJackson;

public class TestAndFilter {
	// A short toString not to prevail is composition .toString
	@Test
	public void toString_grandTotal() {
		ISliceFilter filter = ISliceFilter.MATCH_ALL;

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
				.doesNotContain("32")
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
	public void testAndOr_onlyOr() {
		Assertions
				.assertThat(AndFilter.and(OrFilter.or(ImmutableMap.of("a", "a1", "b", "b1")),
						OrFilter.or(ImmutableMap.of("a", "a1", "b", "b2"))))
				.isEqualTo(AndFilter.and(Map.of("a", "a1")));
	}

	@Test
	public void testAndFilters_twoGrandTotal() {
		ISliceFilter filterAllAndA = AndFilter.and(ISliceFilter.MATCH_ALL, ISliceFilter.MATCH_ALL);

		Assertions.assertThat(filterAllAndA).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testAndFilters_oneGrandTotal() {
		ISliceFilter filterAllAndA = AndFilter.and(ISliceFilter.MATCH_ALL, ColumnFilter.isEqualTo("a", "a1"));

		Assertions.assertThat(filterAllAndA).isEqualTo(ColumnFilter.isEqualTo("a", "a1"));
	}

	@Test
	public void testAndFilters_oneGrandTotal_forced() {
		ISliceFilter filterAllAndA =
				AndFilter.builder().filter(ISliceFilter.MATCH_NONE).filter(ColumnFilter.isEqualTo("a", "a1")).build();

		// We forced an AndFilter: It is not simplified into IAdhocFilter.MATCH_NONE but is is isMatchNone
		Assertions.assertThat(filterAllAndA.isMatchNone()).isTrue();
		Assertions.assertThat(filterAllAndA).isNotEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testAndFilters_oneGrandTotal_TwoCustom() {
		ISliceFilter filterAllAndA =
				AndFilter.and(ISliceFilter.MATCH_ALL, ColumnFilter.isLike("a", "%a"), ColumnFilter.isLike("a", "a%"));

		Assertions.assertThat(filterAllAndA).isInstanceOfSatisfying(AndFilter.class, andF -> {
			Assertions.assertThat(andF.getOperands())
					.hasSize(2)
					.contains(ColumnFilter.isLike("a", "%a"), ColumnFilter.isLike("a", "a%"));
		});
	}

	@Test
	public void testMultipleSameColumn_equalsAndIn() {
		ISliceFilter filterA1andInA12 =
				AndFilter.and(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isIn("a", "a1", "a2"));

		Assertions.assertThat(filterA1andInA12).isEqualTo(ColumnFilter.isEqualTo("a", "a1"));
	}

	@Test
	public void testMultipleSameColumn_InAndIn() {
		ISliceFilter filterA1andInA12 =
				AndFilter.and(ColumnFilter.isIn("a", "a1", "a2"), ColumnFilter.isIn("a", "a2", "a3"));

		Assertions.assertThat(filterA1andInA12).isEqualTo(ColumnFilter.isEqualTo("a", "a2"));
	}

	@Test
	public void testMultipleSameColumn_InAndIn_3blocks() {
		ISliceFilter filterA1andInA12 = AndFilter.and(ColumnFilter.isIn("a", "a1", "a2"),
				ColumnFilter.isIn("a", "a3", "a4"),
				ColumnFilter.isIn("a", "a5", "a6"));

		Assertions.assertThat(filterA1andInA12).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		ISliceFilter filter = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b2"));

		ObjectMapper objectMapper = JsonMapper.builder().build();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		objectMapper.registerModule(AdhocPublicJackson.makeModule());

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

		ISliceFilter fromString = objectMapper.readValue(asString, ISliceFilter.class);

		Assertions.assertThat(fromString).isEqualTo(filter);
	}

	@Test
	public void testJackson_empty() throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		objectMapper.registerModule(AdhocPublicJackson.makeModule());

		ISliceFilter fromString = objectMapper.readValue("{}", ISliceFilter.class);

		Assertions.assertThat(fromString).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testJackson_matchAll() throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		objectMapper.registerModule(AdhocPublicJackson.makeModule());

		String asString = objectMapper.writeValueAsString(ISliceFilter.MATCH_ALL);
		Assertions.assertThat(asString).isEqualTo("""
				"matchAll"
								""".trim());

		ISliceFilter fromString = objectMapper.readValue(asString, ISliceFilter.class);

		Assertions.assertThat(fromString).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testChained() {
		ISliceFilter a1Andb2 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b2"));
		ISliceFilter a1Andb2AndC3 = AndFilter.and(a1Andb2, ColumnFilter.isEqualTo("c", "c3"));

		Assertions.assertThat(a1Andb2AndC3).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands()).hasSize(3);
		});
	}

	@Test
	public void testMultipleEqualsSameColumn_disjoint() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("a", "a2"));

		Assertions.assertThat(a1Anda2).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testMultipleEqualsSameColumn_joint() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("a", "a1"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.isEqualTo("a", "a1"));
	}

	@Test
	public void testMultipleEqualsSameColumn_deep() {
		ISliceFilter a1Andb1 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b1"));

		ISliceFilter a1Andb1AndB2 = AndFilter.and(a1Andb1, ColumnFilter.isEqualTo("b", "b2"));

		Assertions.assertThat(a1Andb1AndB2).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testMultipleEqualsSameColumn_joint_andComplexNotColumn() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"),
				ColumnFilter.isEqualTo("a", "a1"),
				FilterBuilder.or(ColumnFilter.isLike("a", "%a"), ColumnFilter.isLike("a", "a%")).optimize());

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.isEqualTo("a", "a1"));
	}

	@Test
	public void testMultipleEqualsSameColumn_joint_andComplexColumn() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"),
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
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"),
				ColumnFilter.isEqualTo("a", "a1"),
				FilterBuilder.or(ColumnFilter.isEqualTo("a", "a2"), ColumnFilter.isEqualTo("a", "a3")).optimize());

		Assertions.assertThat(a1Anda2)
				.isEqualTo(AndFilter.and(ColumnFilter.isEqualTo("a", "a1"),
						FilterBuilder.or(ColumnFilter.isEqualTo("a", "a2"), ColumnFilter.isEqualTo("a", "a3"))
								.optimize()));
	}

	@Test
	public void testMultipleInSameColumn_disjoint() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.isIn("a", "a1", "a2"), ColumnFilter.isIn("a", "a3", "a4"));

		Assertions.assertThat(a1Anda2).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testMultipleInSameColumn_joint_single() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.isIn("a", "a1", "a2"), ColumnFilter.isIn("a", "a2", "a3"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.isEqualTo("a", "a2"));
	}

	@Test
	public void testMultipleInSameColumn_joint_multiple() {
		ISliceFilter a1Anda2 =
				AndFilter.and(ColumnFilter.isIn("a", "a1", "a2", "a3"), ColumnFilter.isIn("a", "a2", "a3", "a4"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.isIn("a", "a2", "a3"));
	}

	@Test
	public void testGroupByColumn_MultipleComplex_In() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.isIn("a", "a1", "a2"),
				ColumnFilter.isLike("a", "a%"),
				ColumnFilter.isLike("a", "%1"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.isEqualTo("a", "a1"));
	}

	@Test
	public void testGroupByColumn_MultipleComplex_Out_withExplicitLeftover() {
		ISliceFilter a1Anda2 = AndFilter.and(NotFilter.not(ColumnFilter.isIn("a", "a11", "a21", "b22")),
				ColumnFilter.isLike("a", "a%"),
				ColumnFilter.isLike("a", "%1"));

		Assertions.assertThat(a1Anda2)
				.isEqualTo(AndFilter.and(NotFilter.not(ColumnFilter.isIn("a", "a11", "a21")),
						ColumnFilter.isLike("a", "a%"),
						ColumnFilter.isLike("a", "%1")));
	}

	@Test
	public void testGroupByColumn_MultipleComplex_Out_withoutExplicitLeftover() {
		ISliceFilter a1Anda2 = AndFilter.and(NotFilter.not(ColumnFilter.isIn("a", "b2", "b22")),
				ColumnFilter.isLike("a", "a%"),
				ColumnFilter.isLike("a", "%1"));

		Assertions.assertThat(a1Anda2)
				.isEqualTo(AndFilter.and(ColumnFilter.isLike("a", "a%"), ColumnFilter.isLike("a", "%1")));
	}

	@Test
	public void testGroupByColumn_MultipleComplex_InAndOut() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.isIn("a", "a1", "a2", "a21"),
				NotFilter.not(ColumnFilter.isIn("a", "a11", "a21")),
				ColumnFilter.isLike("a", "a%"),
				ColumnFilter.isLike("a", "%1"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.isEqualTo("a", "a1"));
	}

	@Test
	public void testEquals_differentOrders() {
		ISliceFilter f1Then2 = AndFilter.and(ColumnFilter.isEqualTo("c1", "v1"), ColumnFilter.isEqualTo("c2", "v2"));
		ISliceFilter f2Then1 = AndFilter.and(ColumnFilter.isEqualTo("c2", "v2"), ColumnFilter.isEqualTo("c1", "v1"));

		Assertions.assertThat(f1Then2).isEqualTo(f2Then1);
	}

	@Test
	public void testFromMap() {
		// size==0
		Assertions.assertThat(AndFilter.and(Map.of())).isEqualTo(ISliceFilter.MATCH_ALL);
		// size==1
		Assertions.assertThat(AndFilter.and(Map.of("c1", "v1"))).isEqualTo(ColumnFilter.isEqualTo("c1", "v1"));
		// size==2
		Assertions.assertThat(AndFilter.and(Map.of("c1", "v1", "c2", "v2")))
				.isEqualTo(AndFilter.and(ColumnFilter.isEqualTo("c1", "v1"), ColumnFilter.isEqualTo("c2", "v2")));
	}

	@Test
	public void testAnd_allNotFilter() {
		ISliceFilter notA1AndNotA2 = AndFilter.and(NotFilter.not(ColumnFilter.isIn("a", "a1", "a2")),
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
		List<ISliceFilter> nots = Arrays.asList(NotFilter.not(ColumnFilter.isLike("a", "a%")),
				NotFilter.not(ColumnFilter.isLike("b", "b%")));
		ISliceFilter notA1AndNotA2 = AndFilter.and(nots);

		Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands()).containsAll(nots);
		});
	}

	@Test
	public void testAnd_allNotFilter_like_3() {
		List<ISliceFilter> likes =
				List.of(ColumnFilter.isLike("a", "a%"), ColumnFilter.isLike("b", "b%"), ColumnFilter.isLike("c", "c%"));
		List<ISliceFilter> nots = likes.stream().map(NotFilter::not).toList();

		// And over 3 Not
		Assertions.assertThat(FilterOptimizerHelpers.costFunction(AndFilter.builder().filters(nots).build()))
				.isEqualTo(3 + 3 + 3);

		// Not over Or over 3 simple: cost==8
		Assertions
				.assertThat(FilterOptimizerHelpers
						.costFunction(NotFilter.builder().negated(OrFilter.builder().filters(likes).build()).build()))
				.isEqualTo(2 + 2 + 1 + 1 + 1);

		ISliceFilter notA1AndNotA2 = AndFilter.and(nots);

		Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(NotFilter.class, notFilter -> {
			// cost==7, which is cheaper than 8
			Assertions.assertThat(FilterOptimizerHelpers.costFunction(notFilter)).isEqualTo(2 + 2 + 1 + 1 + 1);

			Assertions.assertThat(notFilter.getNegated()).isInstanceOfSatisfying(OrFilter.class, orFilter -> {
				Assertions.assertThat(orFilter.getOperands()).containsAll(likes);
			});
		});
	}

	@Test
	public void testAnd_allNotMatcher() {
		ISliceFilter notA1AndNotB1 =
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
		ISliceFilter notA1AndNotA2 =
				AndFilter.and(NotFilter.not(ColumnFilter.isIn("a", "a1", "a2")), ColumnFilter.isIn("b", "b1", "b2"));
		Assertions.assertThat(notA1AndNotA2).isInstanceOf(AndFilter.class);
		Assertions.assertThat(AndFilter.and(notA1AndNotA2, notA1AndNotA2)).isInstanceOf(AndFilter.class);
	}

	@Test
	public void testAnd_orDifferentColumns_sameColumn() {
		ColumnFilter left = ColumnFilter.isEqualTo("g", "c1");
		ISliceFilter leftOrRight = FilterBuilder.or(left, ColumnFilter.isEqualTo("h", "c1")).optimize();

		Assertions.assertThat(AndFilter.and(leftOrRight, left)).isEqualTo(left);
	}

	@Test
	public void testCostFunction() {
		Assertions.assertThat(FilterOptimizerHelpers.costFunction(OrFilter.or(Map.of("a", "a1")))).isEqualTo(1);
		Assertions.assertThat(FilterOptimizerHelpers.costFunction(AndFilter.and(Map.of("a", "a1")))).isEqualTo(1);

		Assertions
				.assertThat(
						FilterBuilder
								.or(AndFilter.and(ImmutableMap.of("a", "a1")),
										AndFilter.and(ImmutableMap.of("b", "b1", "c", "c1")))
								.optimize())
				.hasToString("a==a1|b==b1&c==c1")
				.satisfies(f -> {
					Assertions.assertThat(FilterOptimizerHelpers.costFunction(f)).isEqualTo(1 + 2 + 1 + 1);
				});

		Assertions
				.assertThat(AndFilter.and(AndFilter.and(ImmutableMap.of("a", "a1")),
						OrFilter.or(ImmutableMap.of("b", "b1", "c", "c1"))))
				.hasToString("a==a1&(b==b1|c==c1)")
				.satisfies(f -> {
					Assertions.assertThat(FilterOptimizerHelpers.costFunction(f)).isEqualTo(1 + 1 + 2 + 1);
				});

		Assertions
				.assertThat(AndFilter.and(AndFilter.and(ImmutableMap.of("a", "a1")),
						FilterBuilder.or(NotFilter.not(ColumnFilter.isLike("b", "b%")), ColumnFilter.isLike("c", "c%"))
								.optimize()))
				// .hasToString("a==a1&(b does NOT match `LikeMatcher(pattern=b%)`|c matches
				// `LikeMatcher(pattern=c%)`)")
				.hasToString("a==a1&!(b matches `LikeMatcher(pattern=b%)`&c does NOT match `LikeMatcher(pattern=c%)`)")
				.satisfies(f -> {
					Assertions.assertThat(FilterOptimizerHelpers.costFunction(f)).isEqualTo(1 + 2 + 1 + 2 + 1);
				});
	}

	@Test
	public void testCostFunction_notOfNot() {
		// `a==a1`
		Assertions
				.assertThat(FilterOptimizerHelpers
						.costFunction(ColumnFilter.isMatching("c", EqualsMatcher.isEqualTo(123L))))
				.isEqualTo(1);

		// `a!=a1`
		Assertions.assertThat(FilterOptimizerHelpers.costFunction(
				ColumnFilter.isMatching("c", NotMatcher.builder().negated(EqualsMatcher.isEqualTo(123L)).build())))
				.isEqualTo(2 + 1);

		// `!(a==a1)`
		Assertions.assertThat(FilterOptimizerHelpers.costFunction(
				NotFilter.builder().negated(ColumnFilter.isMatching("c", EqualsMatcher.isEqualTo(123L))).build()))
				.isEqualTo(2 + 1);

		// `not(not(a==a1))`
		Assertions.assertThat(FilterOptimizerHelpers.costFunction(ColumnFilter.isMatching("c",
				NotMatcher.builder()
						.negated(NotMatcher.builder().negated(EqualsMatcher.isEqualTo(123L)).build())
						.build())))
				.isEqualTo(2 + 2 + 1);
	}

	@Test
	public void testAnd_manyNegationsAndRedundant() {
		ISliceFilter notA_and_notB = AndFilter.and(NotFilter.not(ColumnFilter.isEqualTo("a", "a1")),
				NotFilter.not(ColumnFilter.isEqualTo("b", "b1")),
				NotFilter.not(ColumnFilter.isEqualTo("c", "c1")));

		// Ensure the wide AND of NOT is turned into a NOT of OR.
		Assertions.assertThat(notA_and_notB).isInstanceOf(NotFilter.class);

		Assertions.assertThat(AndFilter.and(notA_and_notB, NotFilter.not(ColumnFilter.isEqualTo("a", "a1"))))
				.isEqualTo(notA_and_notB);
	}

	@Test
	public void testAnd_Large_onlyOrs() {
		{
			List<ISliceFilter> operands = IntStream.range(0, 1)
					.mapToObj(i -> OrFilter.or(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
					.toList();

			Assertions.assertThat(AndFilter.and(operands)).hasToString("a==a0|b==b0");
		}
		{
			List<ISliceFilter> operands = IntStream.range(0, 2)
					.mapToObj(i -> OrFilter.or(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
					.toList();

			Assertions.assertThat(AndFilter.and(operands)).hasToString("a==a0&b==b1|b==b0&a==a1");
		}
		{
			List<ISliceFilter> operands = IntStream.range(0, 3)
					.mapToObj(i -> OrFilter.or(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
					.toList();

			Assertions.assertThat(AndFilter.and(operands)).hasToString("matchNone");
		}
		{
			List<ISliceFilter> operands = IntStream.range(0, 16)
					.mapToObj(i -> OrFilter.or(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
					.toList();

			// The combination is too large to detect it is matchNone
			Assertions.assertThat(AndFilter.and(operands))
					.hasToString(
							"(a==a0|b==b0)&(a==a1|b==b1)&(a==a2|b==b2)&(a==a3|b==b3)&(a==a4|b==b4)&(a==a5|b==b5)&(a==a6|b==b6)&(a==a7|b==b7)&(a==a8|b==b8)&(a==a9|b==b9)&(a==a10|b==b10)&(a==a11|b==b11)&(a==a12|b==b12)&(a==a13|b==b13)&(a==a14|b==b14)&(a==a15|b==b15)");
		}
	}

	@Test
	public void testAnd_Large_andMatchOnlyOne() {
		List<ISliceFilter> operands = IntStream.range(0, 16)
				.mapToObj(i -> OrFilter.or(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
				.collect(Collectors.toCollection(ArrayList::new));

		// Adding a simpler operand may help detecting this is matchNone
		operands.add(AndFilter.and(ImmutableMap.of("a", "a" + 0)));

		Assertions.assertThat(AndFilter.and(operands)).isEqualTo(ISliceFilter.MATCH_NONE);
	}
}
