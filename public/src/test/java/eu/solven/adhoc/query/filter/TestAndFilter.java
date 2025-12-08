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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.query.filter.FilterBuilder.Type;
import eu.solven.adhoc.query.filter.optimizer.FilterOptimizer;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer.IOptimizerEventListener;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import eu.solven.adhoc.resource.AdhocPublicJackson;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.PerformanceGateway;

public class TestAndFilter {
	AtomicInteger nbSkip = new AtomicInteger();
	IOptimizerEventListener listener = new IOptimizerEventListener() {

		@Override
		public void onSkip(ISliceFilter filter) {
			nbSkip.incrementAndGet();
		}
	};
	FilterOptimizer optimizer = FilterOptimizer.builder().listener(listener).build();

	// A short toString not to prevail is composition .toString
	@Test
	public void toString_grandTotal() {
		ISliceFilter filter = ISliceFilter.MATCH_ALL;

		Assertions.assertThat(filter.toString()).isEqualTo("matchAll");
		Assertions.assertThat(filter.isMatchAll()).isTrue();

		Assertions.assertThat(AndFilter.and(Map.of())).isInstanceOf(AndFilter.class).hasToString("matchAll");
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
		ISliceFilter filterAllAndA = AndFilter.and(ISliceFilter.MATCH_ALL, ColumnFilter.matchEq("a", "a1"));

		Assertions.assertThat(filterAllAndA).isEqualTo(ColumnFilter.matchEq("a", "a1"));
	}

	@Test
	public void testAndFilters_oneGrandTotal_forced() {
		ISliceFilter filterAllAndA =
				AndFilter.builder().and(ISliceFilter.MATCH_NONE).and(ColumnFilter.matchEq("a", "a1")).build();

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
				AndFilter.and(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchIn("a", "a1", "a2"));

		Assertions.assertThat(filterA1andInA12).isEqualTo(ColumnFilter.matchEq("a", "a1"));
	}

	@Test
	public void testMultipleSameColumn_InAndIn() {
		ISliceFilter filterA1andInA12 =
				AndFilter.and(ColumnFilter.matchIn("a", "a1", "a2"), ColumnFilter.matchIn("a", "a2", "a3"));

		Assertions.assertThat(filterA1andInA12).isEqualTo(ColumnFilter.matchEq("a", "a2"));
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
		ISliceFilter filter = AndFilter.and(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b2"));

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
		ISliceFilter a1Andb2 =
				FilterBuilder.and(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b2")).combine();
		ISliceFilter a1Andb2AndC3 = FilterBuilder.and(a1Andb2, ColumnFilter.matchEq("c", "c3")).optimize();

		Assertions.assertThat(a1Andb2AndC3).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands()).hasSize(3);
		});
	}

	@Test
	public void testMultipleEqualsSameColumn_disjoint() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("a", "a2"));

		Assertions.assertThat(a1Anda2).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testMultipleEqualsSameColumn_joint() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("a", "a1"));

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.matchEq("a", "a1"));
	}

	@Test
	public void testMultipleEqualsSameColumn_deep() {
		ISliceFilter a1Andb1 = AndFilter.and(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b1"));

		ISliceFilter a1Andb1AndB2 = AndFilter.and(a1Andb1, ColumnFilter.matchEq("b", "b2"));

		Assertions.assertThat(a1Andb1AndB2).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testMultipleEqualsSameColumn_joint_andComplexNotColumn() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.matchEq("a", "a1"),
				ColumnFilter.matchEq("a", "a1"),
				FilterBuilder.or(ColumnFilter.matchLike("a", "%a"), ColumnFilter.matchLike("a", "a%")).optimize());

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.matchEq("a", "a1"));
	}

	@Test
	public void testMultipleEqualsSameColumn_joint_andComplexColumn() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.matchEq("a", "a1"),
				ColumnFilter.matchEq("a", "a1"),
				ColumnFilter.builder()
						.column("a")
						.valueMatcher(OrMatcher.builder()
								.or(LikeMatcher.matching("%ab"))
								.or(LikeMatcher.matching("a%"))
								.build())
						.build());

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.matchEq("a", "a1"));
	}

	@Test
	public void testMultipleEqualsSameColumn_joint_withOr() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.matchEq("a", "a1"),
				ColumnFilter.matchEq("a", "a1"),
				FilterBuilder.or(ColumnFilter.matchEq("a", "a2"), ColumnFilter.matchEq("a", "a3")).optimize());

		Assertions.assertThat(a1Anda2)
				.isEqualTo(AndFilter.and(ColumnFilter.matchEq("a", "a1"),
						FilterBuilder.or(ColumnFilter.matchEq("a", "a2"), ColumnFilter.matchEq("a", "a3")).optimize()));
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

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.matchEq("a", "a2"));
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

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.matchEq("a", "a1"));
	}

	@Test
	public void testGroupByColumn_MultipleComplex_Out_withExplicitLeftover() {
		ISliceFilter a1Anda2 =
				FilterBuilder
						.and(ColumnFilter.matchIn("a", "a11", "a21", "b22").negate(),
								ColumnFilter.matchLike("a", "a%"),
								ColumnFilter.matchLike("a", "%1"))
						.optimize();

		Assertions.assertThat(a1Anda2)
				.isEqualTo(
						FilterBuilder
								.and(ColumnFilter.matchIn("a", "a11", "a21").negate(),
										ColumnFilter.matchLike("a", "a%"),
										ColumnFilter.matchLike("a", "%1"))
								.optimize());
	}

	@Test
	public void testGroupByColumn_MultipleComplex_Out_withoutExplicitLeftover() {
		ISliceFilter a1Anda2 = AndFilter.and(ColumnFilter.matchIn("a", "b2", "b22").negate(),
				ColumnFilter.matchLike("a", "a%"),
				ColumnFilter.matchLike("a", "%1"));

		Assertions.assertThat(a1Anda2)
				.isEqualTo(AndFilter.and(ColumnFilter.matchLike("a", "a%"), ColumnFilter.matchLike("a", "%1")));
	}

	@Test
	public void testGroupByColumn_MultipleComplex_InAndOut() {
		ISliceFilter a1Anda2 =
				FilterBuilder
						.and(ColumnFilter.matchIn("a", "a1", "a2", "a21"),
								ColumnFilter.matchIn("a", "a11", "a21").negate(),
								ColumnFilter.matchLike("a", "a%"),
								ColumnFilter.matchLike("a", "%1"))
						.optimize();

		Assertions.assertThat(a1Anda2).isEqualTo(ColumnFilter.matchEq("a", "a1"));
	}

	@Test
	public void testEquals_differentOrders() {
		ISliceFilter f1Then2 =
				FilterBuilder.and(ColumnFilter.matchEq("c1", "v1"), ColumnFilter.matchEq("c2", "v2")).optimize();
		ISliceFilter f2Then1 =
				FilterBuilder.and(ColumnFilter.matchEq("c2", "v2"), ColumnFilter.matchEq("c1", "v1")).optimize();

		Assertions.assertThat(f1Then2).isEqualTo(f2Then1);
	}

	@Test
	public void testFromMap() {
		// size==0
		Assertions.assertThat(AndFilter.and(Map.of())).isEqualTo(ISliceFilter.MATCH_ALL);
		// size==1
		Assertions.assertThat(AndFilter.and(Map.of("c1", "v1"))).isEqualTo(ColumnFilter.matchEq("c1", "v1"));
		// size==2
		Assertions.assertThat(AndFilter.and(Map.of("c1", "v1", "c2", "v2")))
				.isEqualTo(AndFilter.and(ColumnFilter.matchEq("c1", "v1"), ColumnFilter.matchEq("c2", "v2")));
	}

	@Test
	public void testAnd_allNotFilter() {
		ISliceFilter notA1AndNotA2 = FilterBuilder
				.and(ColumnFilter.matchIn("a", "a1", "a2").negate(), ColumnFilter.matchIn("b", "b1", "b2").negate())
				.optimize();

		Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands())
					.contains(ColumnFilter.matchIn("a", "a1", "a2").negate(),
							ColumnFilter.matchIn("b", "b1", "b2").negate());
		});
	}

	@Test
	public void testAnd_allNotFilter_like_2() {
		List<ISliceFilter> nots =
				Arrays.asList(ColumnFilter.matchLike("a", "a%").negate(), ColumnFilter.matchLike("b", "b%").negate());
		ISliceFilter notA1AndNotA2 = FilterBuilder.and(nots).optimize();

		Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands()).containsAll(nots);
		});
	}

	@Test
	public void testAnd_allNotMatcher() {
		ISliceFilter notA1AndNotB1 =
				FilterBuilder.and(ColumnFilter.notEq("a", "a1"), ColumnFilter.notEq("b", "b1")).optimize();

		// TODO Should we simplify the nots?
		// Assertions.assertThat(notA1AndNotA2).isInstanceOfSatisfying(NotFilter.class, notFilter -> {
		// Assertions.assertThat(notFilter.getNegated())
		// .isEqualTo(NotFilter
		// .not(OrFilter.or(ColumnFilter.isIn("a", "a1", "a2"), ColumnFilter.isIn("b", "b1", "b2"))));
		// });

		Assertions.assertThat(notA1AndNotB1).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands())
					.contains(ColumnFilter.notEq("a", "a1"), ColumnFilter.notEq("b", "b1"));
		});
	}

	@Test
	public void testAnd_partialNot() {
		ISliceFilter notA1AndNotA2 =
				FilterBuilder.and(ColumnFilter.matchIn("a", "a1", "a2").negate(), ColumnFilter.matchIn("b", "b1", "b2"))
						.optimize();
		Assertions.assertThat(notA1AndNotA2).isInstanceOf(AndFilter.class);
		Assertions.assertThat(FilterBuilder.and(notA1AndNotA2, notA1AndNotA2).optimize()).isInstanceOf(AndFilter.class);
	}

	@Test
	public void testAnd_orDifferentColumns_sameColumn() {
		ISliceFilter left = ColumnFilter.matchEq("g", "c1");
		ISliceFilter leftOrRight = FilterBuilder.or(left, ColumnFilter.matchEq("h", "c1")).combine();

		Assertions.assertThat(FilterBuilder.and(leftOrRight, left).optimize()).isEqualTo(left);
	}

	@Test
	public void testCostFunction_manyNot() {
		ISliceFilter output = FilterBuilder.and()
				.filter(ColumnFilter.notEq("b", "b1"))
				.filter(ColumnFilter.notEq("c", "c1"))
				.filter(ColumnFilter.notEq("d", "d1"))
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
				.filter(FilterBuilder.and(ColumnFilter.matchEq("d", "d1"), notLikeA1).combine().negate())
				.filter(FilterBuilder.and(ColumnFilter.notIn("b", "b3", "b4"), ColumnFilter.matchEq("d", "d2"))
						.combine()
						.negate())
				.optimize();

		Assertions.assertThat(output).hasToString("c=out=(c1,c2)&b=out=(b1,b2,b3,b4)&d=out=(d1,d2)&a NOT LIKE 'a1'");
	}

	@Test
	public void testAnd_manyNegationsAndRedundant_equal() {
		ISliceFilter notA_and_notB = AndFilter
				.and(ColumnFilter.notEq("a", "a1"), ColumnFilter.notEq("b", "b1"), ColumnFilter.notEq("c", "c1"));

		// Ensure the wide AND of NOT is turned into a NOT of OR.
		Assertions.assertThat(notA_and_notB).isInstanceOf(AndFilter.class).hasToString("a!=a1&b!=b1&c!=c1");

		Assertions.assertThat(AndFilter.and(notA_and_notB, ColumnFilter.notEq("a", "a1"))).isEqualTo(notA_and_notB);
	}

	@Test
	public void testAnd_manyNegationsAndRedundant_like() {
		ISliceFilter notAB = AndFilter.and(ColumnFilter.notLike("a", "a%"), ColumnFilter.notLike("b", "b%"));

		// In not many negated operators, we stick to an AND(many nots)
		Assertions.assertThat(notAB).isInstanceOf(AndFilter.class).hasToString("a NOT LIKE 'a%'&b NOT LIKE 'b%'");

		ISliceFilter notABC = AndFilter
				.and(ColumnFilter.notLike("a", "a%"), ColumnFilter.notLike("b", "b%"), ColumnFilter.notLike("c", "c%"));

		// In enough negated operators, we prefer a NOT of positive operators
		Assertions.assertThat(notABC)
				.isInstanceOf(NotFilter.class)
				.hasToString("!(a LIKE 'a%'|b LIKE 'b%'|c LIKE 'c%')");
	}

	@Test
	public void testAnd_Large_onlyOrs() {
		FilterOptimizer optimizer = FilterOptimizer.builder().withCartesianProductsAndOr(true).build();
		FilterBuilder filterBuilder = FilterBuilder.builder().andElseOr(Type.AND).build();

		{
			List<ISliceFilter> operands = IntStream.range(0, 1)
					.mapToObj(i -> OrFilter.or(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
					.toList();

			Assertions.assertThat(filterBuilder.filters(operands).optimize(optimizer)).hasToString("a==a0|b==b0");
		}
		{
			List<ISliceFilter> operands = IntStream.range(0, 2)
					.mapToObj(i -> OrFilter.or(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
					.toList();

			Assertions.assertThat(filterBuilder.filters(operands).optimize(optimizer))
					.hasToString("a==a0&b==b1|b==b0&a==a1");
		}
		{
			List<ISliceFilter> operands = IntStream.range(0, 3)
					.mapToObj(i -> OrFilter.or(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
					.toList();

			Assertions.assertThat(filterBuilder.filters(operands).optimize(optimizer)).hasToString("matchNone");
		}
		{
			// `(a0|b0)&(a1|b1)&(a2|b2)&...`
			List<ISliceFilter> operands = IntStream.range(0, 16)
					.mapToObj(i -> OrFilter.or(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
					.toList();

			// The combination is too large to detect it is matchNone
			Assertions.assertThat(filterBuilder.filters(operands).optimize())
					.hasToString(
							"(a==a0|b==b0)&(a==a1|b==b1)&(a==a2|b==b2)&(a==a3|b==b3)&(a==a4|b==b4)&(a==a5|b==b5)&(a==a6|b==b6)&(a==a7|b==b7)&(a==a8|b==b8)&(a==a9|b==b9)&(a==a10|b==b10)&(a==a11|b==b11)&(a==a12|b==b12)&(a==a13|b==b13)&(a==a14|b==b14)&(a==a15|b==b15)");
		}
	}

	// Similar to `testAnd_Large_onlyOrs` but with a helping filter
	@Test
	public void testAnd_Large_andMatchOnlyOne() {
		FilterOptimizer optimizer = FilterOptimizer.builder().withCartesianProductsAndOr(true).build();

		// In this case, given the hint, we do not need any cartesianProduct
		FilterBuilder filterBuilder = FilterBuilder.builder().andElseOr(Type.AND).build();

		// `(a0|b0)&(a1|b1)&(a2|b2)&...`
		List<ISliceFilter> operands = IntStream.range(0, 16)
				.mapToObj(i -> OrFilter.or(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
				.collect(Collectors.toCollection(ArrayList::new));

		// Adding a simpler operand may help detecting this is matchNone
		operands.add(AndFilter.and(ImmutableMap.of("a", "a" + 0)));

		Assertions.assertThat(filterBuilder.filters(operands).optimize(optimizer)).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testAnd_NotAndFalseGivenContext() {
		List<ISliceFilter> outerAnds = new ArrayList<>();

		outerAnds.add(AndFilter.and(Map.of("a", "a0")));
		outerAnds.add(ColumnFilter.matchIn("b", "b1", "b2"));
		// This following is always true given outer `b=in=b1,b2
		outerAnds.add(AndFilter.builder()
				.and(ColumnFilter.matchEq("c", "c1"))
				// The following is always false given outer `b=in=b1,b2`
				.and(ColumnFilter.matchIn("b", "b1", "b2", "b3").negate())
				.build()
				.negate());

		Assertions.assertThat(AndFilter.and(outerAnds)).hasToString("a==a0&b=in=(b1,b2)");
	}

	@Test
	public void testAnd_packInAndNotIn() {
		List<ISliceFilter> outerAnds = new ArrayList<>();

		outerAnds.add(AndFilter.and(Map.of("a", "a0")));
		outerAnds.add(ColumnFilter.matchIn("b", "b1", "b2"));
		outerAnds.add(ColumnFilter.matchIn("b", "b2", "b3").negate());

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

	// Consider an AND over combined entries: if we optimize the outer, we should re-optimize the inner. It is important
	// to ensure a single representation per boolean expression
	@Test
	public void testAnd_optimizeInputs() {
		ISliceFilter combined = FilterBuilder
				.or(ColumnFilter.notEq("a", "a1"), ColumnFilter.notEq("b", "b1"), ColumnFilter.notEq("c", "c1"))
				.combine();
		ISliceFilter optimized = FilterBuilder
				.or(ColumnFilter.notEq("a", "a1"), ColumnFilter.notEq("b", "b1"), ColumnFilter.notEq("c", "c1"))
				.optimize();

		Assertions.assertThat(combined).isNotEqualTo(optimized);
		// Commented as `FilterEquivalencyHelpers` does not manage this case, due to mis-management of `NOT`
		// Assertions.assertThat(FilterEquivalencyHelpers.areEquivalent(combined, optimized)).isTrue();

		Assertions.assertThat(FilterBuilder.and(ColumnFilter.matchEq("d", "d1"), combined).optimize())
				.isEqualTo(FilterBuilder.and(ColumnFilter.matchEq("d", "d1"), optimized).optimize());

	}

	@Test
	public void testAndOr_contradictoryIns() {
		// b has 3 options, c has 3 options, d has 2 options (post simplification by packing columns)
		AdhocUnsafe.cartesianProductLimit = 3 * 3 * 2;

		try {
			FilterOptimizer optimizer = FilterOptimizer.builder().listener(listener).build();
			ISliceFilter combined =
					FilterBuilder
							.and(ColumnFilter.matchEq("a", "a1"),
									ColumnFilter.matchIn("b", "b1", "b2", "b3"),
									ColumnFilter.matchIn("c", "c1", "c2", "c3"),
									ColumnFilter.notEq("d", "d1"),
									ColumnFilter.matchIn("d", "d1", "d2", "d3"))
							.optimize(optimizer);

			Assertions.assertThat(combined).hasToString("a==a1&b=in=(b1,b2,b3)&c=in=(c1,c2,c3)&d=in=(d2,d3)");
			Assertions.assertThat(nbSkip).hasValue(0);
		} finally {
			AdhocUnsafe.resetProperties();
		}
	}

	@Test
	public void testAndOr_orStricterThanSimplerAnd() {
		ISliceFilter combined =
				FilterBuilder
						.and(ColumnFilter.matchIn("a", "a1", "a2", "a3"),
								ColumnFilter.matchIn("b", "b1", "b2", "b3"),
								FilterBuilder
										.or(ColumnFilter.matchIn("a", "a1", "a2", "a4"),
												ColumnFilter.matchIn("b", "b1", "b2", "b4"))
										.combine())
						.optimize(optimizer);

		Assertions.assertThat(combined).hasToString("a=in=(a1,a2,a3)&b=in=(b1,b2,b3)&(a=in=(a1,a2)|b=in=(b1,b2))");
		Assertions.assertThat(nbSkip).hasValue(0);
	}

	@Test
	@PerformanceGateway
	public void testAnd_between2Large_differentColumns() {
		int size = 256;
		ISliceFilter left =
				FilterBuilder.and(IntStream.range(0, size).mapToObj(i -> ColumnFilter.matchEq("a" + i, i)).toList())
						.combine();

		ISliceFilter right =
				FilterBuilder.and(IntStream.range(0, size).mapToObj(i -> ColumnFilter.matchEq("b" + i, i)).toList())
						.combine();

		FilterBuilder.and(left, right).optimize();
	}

	@Test
	@PerformanceGateway
	public void testAnd_OneLargeIn() {
		int size = 256 * 1024;

		ISliceFilter left = ColumnFilter.matchEq("a", "a0");

		// Need at least 2 OR to trigger some optimization pathes in `optimizeAndOfOr`
		ISliceFilter middle = ColumnFilter.matchIn("b", ImmutableSet.of("b1", "b2"));
		ISliceFilter right = ColumnFilter.matchIn("c", ContiguousSet.closedOpen(0, size));

		FilterBuilder.and(left, middle, right).optimize();
	}

	@Test
	@PerformanceGateway
	public void testAnd_OneLargeIn_crossWithSimpleOr() {
		int size = 256 * 1024;

		ISliceFilter left = ColumnFilter.matchEq("a", "a0");

		// Need at least 2 OR to trigger some optimization pathes in `optimizeAndOfOr`
		ISliceFilter middle = ColumnFilter.matchIn("c", ImmutableSet.of(0, 1));
		ISliceFilter right = ColumnFilter.matchIn("c", ContiguousSet.closedOpen(0, size));

		FilterBuilder.and(left, middle, right).optimize();
	}

	@Test
	@PerformanceGateway
	public void testAnd_OneLargeNotIn() {
		int size = 256 * 1024;

		// ISliceFilter left = ColumnFilter.matchEq("a", "a0");

		// Need at least 2 OR to trigger some optimization pathes in `optimizeAndOfOr`
		// ISliceFilter middle = ColumnFilter.matchIn("c", ImmutableSet.of(0, 1));
		ISliceFilter right = ColumnFilter.matchIn("c", ContiguousSet.closedOpen(0, size)).negate();

		FilterBuilder.and(
				// left, middle,
				right).optimize();
	}

	@Test
	public void testAndOr_orStricterThanSimplerAnd_notAroundOr() {
		ISliceFilter raw = FilterBuilder.and()
				.filter(ColumnFilter.matchIn("a", "a1", "a2", "a3"))
				.filter(NotFilter.builder()
						.negated(AndFilter.builder()
								.and(ColumnFilter.notIn("b", "b1", "b2"))
								.and(

										NotFilter.builder()
												.negated(AndFilter.builder()
														.and(ColumnFilter.matchEq("c", "c1"))
														.and(ColumnFilter.matchEq("d", "d1"))
														.and(ColumnFilter.matchEq("e", "e1"))
														.build())
												.build())
								.build()

						)
						.build())

				.combine();
		Assertions.assertThat(raw).hasToString("a=in=(a1,a2,a3)&!(b=out=(b1,b2)&!(c==c1&d==d1&e==e1))");

		ISliceFilter optimized = FilterBuilder.and(raw).optimize(optimizer);

		Assertions.assertThat(optimized).hasToString("a=in=(a1,a2,a3)&(b=in=(b1,b2)|c==c1&d==d1&e==e1)");
		Assertions.assertThat(nbSkip).hasValue(0);
	}
}
