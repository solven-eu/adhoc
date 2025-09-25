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
	FilterOptimizerHelpers optimizer = new FilterOptimizerHelpers();

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
		ISliceFilter filterAllAndA = AndFilter.and(ISliceFilter.MATCH_ALL, ColumnFilter.equalTo("a", "a1"));

		Assertions.assertThat(filterAllAndA).isEqualTo(ColumnFilter.equalTo("a", "a1"));
	}

	@Test
	public void testAndFilters_oneGrandTotal_forced() {
		ISliceFilter filterAllAndA =
				AndFilter.builder().and(ISliceFilter.MATCH_NONE).and(ColumnFilter.equalTo("a", "a1")).build();

		// We forced an AndFilter: It is not simplified into IAdhocFilter.MATCH_NONE but is is isMatchNone
		Assertions.assertThat(filterAllAndA.isMatchNone()).isTrue();
		Assertions.assertThat(filterAllAndA).isNotEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testAndFilters_oneGrandTotal_TwoCustom() {
		ISliceFilter filterAllAndA = AndFilter
				.and(ISliceFilter.MATCH_ALL, ColumnFilter.matchLike("a", "%a"), ColumnFilter.matchLike("a", "a%"));

		Assertions.assertThat(filterAllAndA).isInstanceOfSatisfying(AndFilter.class, andF -> {
			Assertions.assertThat(andF.getOperands())
					.hasSize(2)
					.contains(ColumnFilter.matchLike("a", "%a"), ColumnFilter.matchLike("a", "a%"));
		});
	}

	@Test
	public void testMultipleSameColumn_equalsAndIn() {
		ISliceFilter filterA1andInA12 =
				AndFilter.and(ColumnFilter.equalTo("a", "a1"), ColumnFilter.matchIn("a", "a1", "a2"));

		Assertions.assertThat(filterA1andInA12).isEqualTo(ColumnFilter.equalTo("a", "a1"));
	}

	@Test
	public void testMultipleSameColumn_InAndIn() {
		ISliceFilter filterA1andInA12 =
				AndFilter.and(ColumnFilter.matchIn("a", "a1", "a2"), ColumnFilter.matchIn("a", "a2", "a3"));

		Assertions.assertThat(filterA1andInA12).isEqualTo(ColumnFilter.equalTo("a", "a2"));
	}

	@Test
	public void testMultipleSameColumn_InAndIn_3blocks() {
		ISliceFilter filterA1andInA12 = AndFilter.and(ColumnFilter.matchIn("a", "a1", "a2"),
				ColumnFilter.matchIn("a", "a3", "a4"),
				ColumnFilter.matchIn("a", "a5", "a6"));

		Assertions.assertThat(filterA1andInA12).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		ISliceFilter filter = AndFilter.and(ColumnFilter.equalTo("a", "a1"), ColumnFilter.equalTo("b", "b2"));

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
		ISliceFilter a1Andb2 = AndFilter.and(ColumnFilter.equalTo("a", "a1"), ColumnFilter.equalTo("b", "b2"));
		ISliceFilter a1Andb2AndC3 = AndFilter.and(a1Andb2, ColumnFilter.equalTo("c", "c3"));

		Assertions.assertThat(a1Andb2AndC3).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands()).hasSize(3);
		});
	}

	@Test
	public void testMultipleEqualsSameColumn_disjoint() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.equalTo("a", "a1"), ColumnFilter.equalTo("a", "a2"));

		Assertions.assertThat(a1Anda2).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testMultipleEqualsSameColumn_joint() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.equalTo("a", "a1"), ColumnFilter.equalTo("a", "a1"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.equalTo("a", "a1"));
	}

	@Test
	public void testMultipleEqualsSameColumn_deep() {
		ISliceFilter a1Andb1 = AndFilter.and(ColumnFilter.equalTo("a", "a1"), ColumnFilter.equalTo("b", "b1"));

		ISliceFilter a1Andb1AndB2 = AndFilter.and(a1Andb1, ColumnFilter.equalTo("b", "b2"));

		Assertions.assertThat(a1Andb1AndB2).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testMultipleEqualsSameColumn_joint_andComplexNotColumn() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.equalTo("a", "a1"),
				ColumnFilter.equalTo("a", "a1"),
				FilterBuilder.or(ColumnFilter.matchLike("a", "%a"), ColumnFilter.matchLike("a", "a%")).optimize());

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.equalTo("a", "a1"));
	}

	@Test
	public void testMultipleEqualsSameColumn_joint_andComplexColumn() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.equalTo("a", "a1"),
				ColumnFilter.equalTo("a", "a1"),
				ColumnFilter.builder()
						.column("a")
						.valueMatcher(OrMatcher.builder()
								.operand(LikeMatcher.matching("%ab"))
								.operand(LikeMatcher.matching("a%"))
								.build())
						.build());

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.equalTo("a", "a1"));
	}

	@Test
	public void testMultipleEqualsSameColumn_joint_withOr() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.equalTo("a", "a1"),
				ColumnFilter.equalTo("a", "a1"),
				FilterBuilder.or(ColumnFilter.equalTo("a", "a2"), ColumnFilter.equalTo("a", "a3")).optimize());

		Assertions.assertThat(a1Anda2)
				.isEqualTo(AndFilter.and(ColumnFilter.equalTo("a", "a1"),
						FilterBuilder.or(ColumnFilter.equalTo("a", "a2"), ColumnFilter.equalTo("a", "a3")).optimize()));
	}

	@Test
	public void testMultipleInSameColumn_disjoint() {
		ISliceFilter a1Anda2 =
				AndFilter.and(ColumnFilter.matchIn("a", "a1", "a2"), ColumnFilter.matchIn("a", "a3", "a4"));

		Assertions.assertThat(a1Anda2).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testMultipleInSameColumn_joint_single() {
		ISliceFilter a1Anda2 =
				AndFilter.and(ColumnFilter.matchIn("a", "a1", "a2"), ColumnFilter.matchIn("a", "a2", "a3"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.equalTo("a", "a2"));
	}

	@Test
	public void testMultipleInSameColumn_joint_multiple() {
		ISliceFilter a1Anda2 =
				AndFilter.and(ColumnFilter.matchIn("a", "a1", "a2", "a3"), ColumnFilter.matchIn("a", "a2", "a3", "a4"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.matchIn("a", "a2", "a3"));
	}

	@Test
	public void testGroupByColumn_MultipleComplex_In() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.matchIn("a", "a1", "a2"),
				ColumnFilter.matchLike("a", "a%"),
				ColumnFilter.matchLike("a", "%1"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.equalTo("a", "a1"));
	}

	@Test
	public void testGroupByColumn_MultipleComplex_Out_withExplicitLeftover() {
		ISliceFilter a1Anda2 = AndFilter.and(NotFilter.not(ColumnFilter.matchIn("a", "a11", "a21", "b22")),
				ColumnFilter.matchLike("a", "a%"),
				ColumnFilter.matchLike("a", "%1"));

		Assertions.assertThat(a1Anda2)
				.isEqualTo(AndFilter.and(NotFilter.not(ColumnFilter.matchIn("a", "a11", "a21")),
						ColumnFilter.matchLike("a", "a%"),
						ColumnFilter.matchLike("a", "%1")));
	}

	@Test
	public void testGroupByColumn_MultipleComplex_Out_withoutExplicitLeftover() {
		ISliceFilter a1Anda2 = AndFilter.and(NotFilter.not(ColumnFilter.matchIn("a", "b2", "b22")),
				ColumnFilter.matchLike("a", "a%"),
				ColumnFilter.matchLike("a", "%1"));

		Assertions.assertThat(a1Anda2)
				.isEqualTo(AndFilter.and(ColumnFilter.matchLike("a", "a%"), ColumnFilter.matchLike("a", "%1")));
	}

	@Test
	public void testGroupByColumn_MultipleComplex_InAndOut() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.matchIn("a", "a1", "a2", "a21"),
				NotFilter.not(ColumnFilter.matchIn("a", "a11", "a21")),
				ColumnFilter.matchLike("a", "a%"),
				ColumnFilter.matchLike("a", "%1"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.equalTo("a", "a1"));
	}

	@Test
	public void testEquals_differentOrders() {
		ISliceFilter f1Then2 = AndFilter.and(ColumnFilter.equalTo("c1", "v1"), ColumnFilter.equalTo("c2", "v2"));
		ISliceFilter f2Then1 = AndFilter.and(ColumnFilter.equalTo("c2", "v2"), ColumnFilter.equalTo("c1", "v1"));

		Assertions.assertThat(f1Then2).isEqualTo(f2Then1);
	}

	@Test
	public void testFromMap() {
		// size==0
		Assertions.assertThat(AndFilter.and(Map.of())).isEqualTo(ISliceFilter.MATCH_ALL);
		// size==1
		Assertions.assertThat(AndFilter.and(Map.of("c1", "v1"))).isEqualTo(ColumnFilter.equalTo("c1", "v1"));
		// size==2
		Assertions.assertThat(AndFilter.and(Map.of("c1", "v1", "c2", "v2")))
				.isEqualTo(AndFilter.and(ColumnFilter.equalTo("c1", "v1"), ColumnFilter.equalTo("c2", "v2")));
	}

	@Test
	public void testAnd_allNotFilter() {
		ISliceFilter notA1AndNotA2 = AndFilter.and(NotFilter.not(ColumnFilter.matchIn("a", "a1", "a2")),
				NotFilter.not(ColumnFilter.matchIn("b", "b1", "b2")));

		// TODO Should we simplify the not?
		// Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(NotFilter.class, notFilter -> {
		// Assertions.assertThat(notFilter.getNegated())
		// .isEqualTo(NotFilter
		// .not(OrFilter.or(ColumnFilter.isIn("a", "a1", "a2"), ColumnFilter.isIn("b", "b1", "b2"))));
		// });

		Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands())
					.contains(NotFilter.not(ColumnFilter.matchIn("a", "a1", "a2")),
							NotFilter.not(ColumnFilter.matchIn("b", "b1", "b2")));
		});
	}

	@Test
	public void testAnd_allNotFilter_like_2() {
		List<ISliceFilter> nots = Arrays.asList(NotFilter.not(ColumnFilter.matchLike("a", "a%")),
				NotFilter.not(ColumnFilter.matchLike("b", "b%")));
		ISliceFilter notA1AndNotA2 = AndFilter.and(nots);

		Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands()).containsAll(nots);
		});
	}

	@Test
	public void testAnd_allNotFilter_like_3() {
		List<ISliceFilter> likes = List.of(ColumnFilter.matchLike("a", "a%"),
				ColumnFilter.matchLike("b", "b%"),
				ColumnFilter.matchLike("c", "c%"));
		List<ISliceFilter> nots = likes.stream().map(NotFilter::not).toList();

		// And over 3 Not
		Assertions.assertThat(optimizer.costFunction(AndFilter.builder().ands(nots).build())).isEqualTo(10 + 10 + 10);

		// Not over Or over 3 simple
		Assertions
				.assertThat(optimizer
						.costFunction(NotFilter.builder().negated(OrFilter.builder().ors(likes).build()).build()))
				.isEqualTo(2 * (5 + (3 + 3 + 3)));

		ISliceFilter notA1AndNotA2 = FilterBuilder.and(nots).optimize();

		Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(NotFilter.class, notFilter -> {
			// cost==7, which is cheaper than 8
			Assertions.assertThat(optimizer.costFunction(notFilter)).isEqualTo(2 * (5 + 3 + 3 + 3));

			Assertions.assertThat(notFilter.getNegated()).isInstanceOfSatisfying(OrFilter.class, orFilter -> {
				Assertions.assertThat(orFilter.getOperands()).containsAll(likes);
			});
		});
	}

	@Test
	public void testAnd_allNotMatcher() {
		ISliceFilter notA1AndNotB1 =
				AndFilter.and(ColumnFilter.notEqualTo("a", "a1"), ColumnFilter.notEqualTo("b", "b1"));

		// TODO Should we simplify the nots?
		// Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(NotFilter.class, notFilter -> {
		// Assertions.assertThat(notFilter.getNegated())
		// .isEqualTo(NotFilter
		// .not(OrFilter.or(ColumnFilter.isIn("a", "a1", "a2"), ColumnFilter.isIn("b", "b1", "b2"))));
		// });

		Assertions.assertThat(notA1AndNotB1).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands())
					.contains(ColumnFilter.notEqualTo("a", "a1"), ColumnFilter.notEqualTo("b", "b1"));
		});
	}

	@Test
	public void testAnd_partialNot() {
		ISliceFilter notA1AndNotA2 = AndFilter.and(NotFilter.not(ColumnFilter.matchIn("a", "a1", "a2")),
				ColumnFilter.matchIn("b", "b1", "b2"));
		Assertions.assertThat(notA1AndNotA2).isInstanceOf(AndFilter.class);
		Assertions.assertThat(AndFilter.and(notA1AndNotA2, notA1AndNotA2)).isInstanceOf(AndFilter.class);
	}

	@Test
	public void testAnd_orDifferentColumns_sameColumn() {
		ColumnFilter left = ColumnFilter.equalTo("g", "c1");
		ISliceFilter leftOrRight = FilterBuilder.or(left, ColumnFilter.equalTo("h", "c1")).optimize();

		Assertions.assertThat(AndFilter.and(leftOrRight, left)).isEqualTo(left);
	}

	@Test
	public void testCostFunction() {
		Assertions.assertThat(optimizer.costFunction(ColumnFilter.equalTo("a", "a1"))).isEqualTo(3);

		Assertions
				.assertThat(
						FilterBuilder
								.or(AndFilter.and(ImmutableMap.of("a", "a1")),
										AndFilter.and(ImmutableMap.of("b", "b1", "c", "c1")))
								.optimize())
				.hasToString("a==a1|b==b1&c==c1")
				.satisfies(f -> {
					Assertions.assertThat(optimizer.costFunction(f)).isEqualTo(3 + 5 + 3 + 3);
				});

		Assertions
				.assertThat(AndFilter.and(AndFilter.and(ImmutableMap.of("a", "a1")),
						OrFilter.or(ImmutableMap.of("b", "b1", "c", "c1"))))
				.hasToString("a==a1&(b==b1|c==c1)")
				.satisfies(f -> {
					Assertions.assertThat(optimizer.costFunction(f)).isEqualTo(3 + 5 + 3 + 3);
				});

		Assertions.assertThat(AndFilter.and(AndFilter.and(ImmutableMap.of("a", "a1")),
				FilterBuilder.or(NotFilter.not(ColumnFilter.matchLike("b", "b%")), ColumnFilter.matchLike("c", "c%"))
						.optimize()))
				// .hasToString("a==a1&(b does NOT match `LikeMatcher(pattern=b%)`|c matches
				// `LikeMatcher(pattern=c%)`)")
				.hasToString("a==a1&(b does NOT match `LikeMatcher(pattern=b%)`|c matches `LikeMatcher(pattern=c%)`)")
				.satisfies(f -> {
					Assertions.assertThat(optimizer.costFunction(f)).isEqualTo(3 + 5 + 10 + 3);
				});
	}

	@Test
	public void testCostFunction_notOfNot() {
		// `a==a1`
		Assertions.assertThat(optimizer.costFunction(ColumnFilter.match("c", EqualsMatcher.equalTo(123L))))
				.isEqualTo(3);

		// `a!=a1`
		Assertions
				.assertThat(optimizer.costFunction(
						ColumnFilter.match("c", NotMatcher.builder().negated(EqualsMatcher.equalTo(123L)).build())))
				.isEqualTo(5);

		// `!(a==a1)`
		Assertions
				.assertThat(optimizer.costFunction(
						NotFilter.builder().negated(ColumnFilter.match("c", EqualsMatcher.equalTo(123L))).build()))
				.isEqualTo(2 * 3);

		// `not(not(a==a1))`
		Assertions.assertThat(optimizer.costFunction(ColumnFilter.match("c",
				NotMatcher.builder()
						.negated(NotMatcher.builder().negated(EqualsMatcher.equalTo(123L)).build())
						.build())))
				.isEqualTo(2 * 2 * 3);
	}

	@Test
	public void testCostFunction_manyNot() {
		ISliceFilter output = FilterBuilder.and()
				.filter(ColumnFilter.notEqualTo("b", "b1"))
				.filter(ColumnFilter.notEqualTo("c", "c1"))
				.filter(ColumnFilter.notEqualTo("d", "d1"))
				.optimize();

		Assertions.assertThat(output).hasToString("b!=b1&c!=c1&d!=d1");
	}

	@Test
	public void testCostFunction_manyOut() {
		ISliceFilter output = FilterBuilder.and()
				.filter(ColumnFilter.notIn("b", "b1", "b2"))
				.filter(ColumnFilter.notIn("c", "c1", "c2"))
				.filter(ColumnFilter.notIn("d", "d1", "d2"))
				.optimize();

		Assertions.assertThat(output).hasToString("b=out=(b1,b2)&c=out=(c1,c2)&d=out=(d1,d2)");
	}

	@Test
	public void testCostFunction_unclear() {
		ISliceFilter notLikeA1 = ColumnFilter.matchLike("a", "a1").negate();
		ISliceFilter output = FilterBuilder.and()
				.filter(notLikeA1)
				.filter(ColumnFilter.notIn("b", "b1", "b2"))
				.filter(ColumnFilter.notIn("c", "c1", "c2"))
				.filter(ColumnFilter.notIn("b", "b3", "b4"))
				.filter(FilterBuilder.and(ColumnFilter.equalTo("d", "d1"), notLikeA1).combine().negate())
				.filter(FilterBuilder.and(ColumnFilter.notIn("b", "b3", "b4"), ColumnFilter.equalTo("d", "d2"))
						.combine()
						.negate())
				.optimize();

		Assertions.assertThat(output)
				.hasToString(
						"b=out=(b1,b2,b3,b4)&c=out=(c1,c2)&d=out=(d1,d2)&a does NOT match `LikeMatcher(pattern=a1)`");
	}

	@Test
	public void testAnd_manyNegationsAndRedundant_equal() {
		ISliceFilter notA_and_notB = AndFilter.and(ColumnFilter.notEqualTo("a", "a1"),
				ColumnFilter.notEqualTo("b", "b1"),
				ColumnFilter.notEqualTo("c", "c1"));

		// Ensure the wide AND of NOT is turned into a NOT of OR.
		Assertions.assertThat(notA_and_notB).isInstanceOf(AndFilter.class).hasToString("a!=a1&b!=b1&c!=c1");

		Assertions.assertThat(AndFilter.and(notA_and_notB, ColumnFilter.notEqualTo("a", "a1")))
				.isEqualTo(notA_and_notB);
	}

	@Test
	public void testAnd_manyNegationsAndRedundant_like() {
		ISliceFilter notAB = AndFilter.and(ColumnFilter.notLike("a", "a%"), ColumnFilter.notLike("b", "b%"));

		// In not many negated operators, we stick to an AND(many nots)
		Assertions.assertThat(notAB)
				.isInstanceOf(AndFilter.class)
				.hasToString("a does NOT match `LikeMatcher(pattern=a%)`&b does NOT match `LikeMatcher(pattern=b%)`");

		ISliceFilter notABC = AndFilter
				.and(ColumnFilter.notLike("a", "a%"), ColumnFilter.notLike("b", "b%"), ColumnFilter.notLike("c", "c%"));

		// In enough negated operators, we prefer a NOT of positive operators
		Assertions.assertThat(notABC)
				.isInstanceOf(NotFilter.class)
				.hasToString(
						"!(a matches `LikeMatcher(pattern=a%)`|b matches `LikeMatcher(pattern=b%)`|c matches `LikeMatcher(pattern=c%)`)");
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

	@Test
	public void testAnd_NotAndFalseGivenContext() {
		List<ISliceFilter> outerAnds = new ArrayList<>();

		outerAnds.add(AndFilter.and(Map.of("a", "a0")));
		outerAnds.add(ColumnFilter.matchIn("b", "b1", "b2"));
		// This following is always true given outer `b=in=b1,b2
		outerAnds.add(NotFilter.not(AndFilter.builder()
				.and(ColumnFilter.equalTo("c", "c1"))
				// The following is always false given outer `b=in=b1,b2`
				.and(NotFilter.not(ColumnFilter.matchIn("b", "b1", "b2", "b3")))
				.build()));

		Assertions.assertThat(AndFilter.and(outerAnds)).hasToString("a==a0&b=in=(b1,b2)");
	}

	@Test
	public void testAnd_packInAndNotIn() {
		List<ISliceFilter> outerAnds = new ArrayList<>();

		outerAnds.add(AndFilter.and(Map.of("a", "a0")));
		outerAnds.add(ColumnFilter.matchIn("b", "b1", "b2"));
		outerAnds.add(NotFilter.not(ColumnFilter.matchIn("b", "b2", "b3")));

		Assertions.assertThat(AndFilter.and(outerAnds)).hasToString("a==a0&b==b1");
	}

	@Test
	public void testAnd_likeAndNotLike() {
		List<ISliceFilter> outerAnds = new ArrayList<>();

		outerAnds.add(ColumnFilter.matchLike("a", "a%"));
		outerAnds.add(ColumnFilter.matchLike("a", "a%").negate());

		Assertions.assertThat(FilterBuilder.and(outerAnds).optimize()).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	// This scenario happens with a Dispatchor given a large group
	@Test
	public void testAnd_largeIn() {
		ISliceFilter largeIn = ColumnFilter.matchIn("c", IntStream.range(0, 256 * 256).mapToObj(i -> i).toList());

		// This may lead to a very slow optimization as the IN may be turned into an OR, in which we'll search for
		// discardable entries.
		Assertions.assertThat(FilterBuilder.and(largeIn, ISliceFilter.MATCH_ALL).optimize()).isEqualTo(largeIn);
	}
}
