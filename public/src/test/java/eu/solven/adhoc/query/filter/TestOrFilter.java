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
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.query.filter.FilterBuilder.Type;
import eu.solven.adhoc.query.filter.optimizer.FilterOptimizer;
import eu.solven.adhoc.query.filter.optimizer.IFilterOptimizer.IOptimizerEventListener;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.resource.AdhocPublicJackson;

public class TestOrFilter {
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
	public void toString_empty() {
		ISliceFilter matchNone = ISliceFilter.MATCH_NONE;
		Assertions.assertThat(matchNone.toString()).isEqualTo("matchNone");

		Assertions.assertThat(matchNone.isMatchAll()).isFalse();
	}

	@Test
	public void toString_notEmpty() {
		ISliceFilter a1orb2 =
				FilterBuilder.or(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b2")).optimize();

		Assertions.assertThat(a1orb2.isMatchAll()).isFalse();
	}

	@Test
	public void toString_small() {
		List<ColumnFilter> filters = IntStream.range(0, 5)
				.mapToObj(
						i -> ColumnFilter.builder().column("k" + i).valueMatcher(LikeMatcher.matching("%" + i)).build())
				.collect(Collectors.toList());

		Assertions.assertThat(FilterBuilder.or(filters).combine().toString())
				.isEqualTo("k0 matches `LikeMatcher(pattern=%0)`" + "|k1 matches `LikeMatcher(pattern=%1)`"
						+ "|k2 matches `LikeMatcher(pattern=%2)`"
						+ "|k3 matches `LikeMatcher(pattern=%3)`"
						+ "|k4 matches `LikeMatcher(pattern=%4)`");
	}

	@Test
	public void toString_huge() {
		List<ColumnFilter> filters = IntStream.range(0, 256)
				.mapToObj(i -> ColumnFilter.builder().column("k").valueMatcher(LikeMatcher.matching("%" + i)).build())
				.collect(Collectors.toList());

		Assertions.assertThat(FilterBuilder.or(filters).combine().toString())
				.contains("#0=k matches `LikeMatcher(pattern=%0)`, #1=k matches `LikeMatcher(pattern=%1)`")
				.doesNotContain("64")
				.hasSizeLessThan(1024);
	}

	@Test
	public void testAndFilters_twoGrandTotal() {
		ISliceFilter filterAllAndA = FilterBuilder.or(ISliceFilter.MATCH_ALL, ISliceFilter.MATCH_ALL).optimize();

		Assertions.assertThat(filterAllAndA).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testAndFilters_oneGrandTotal() {
		ISliceFilter filterAllAndA =
				FilterBuilder.or(ISliceFilter.MATCH_ALL, ColumnFilter.matchEq("a", "a1")).optimize();

		Assertions.assertThat(filterAllAndA).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testOr_oneGrandTotal_forced() {
		ISliceFilter filterAllAndA =
				FilterBuilder.or(ISliceFilter.MATCH_ALL, ColumnFilter.matchEq("a", "a1")).combine();

		// We forced an OrBuilder: It is not simplified into IAdhocFilter.MATCH_ALL but is is isMatchAll
		Assertions.assertThat(filterAllAndA.isMatchAll()).isTrue();
		Assertions.assertThat(filterAllAndA).isNotEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testOr_oneMatchNone() {
		ISliceFilter filterAllAndA =
				FilterBuilder.or(ISliceFilter.MATCH_NONE, ColumnFilter.matchEq("a", "a1")).optimize();

		Assertions.assertThat(filterAllAndA).isEqualTo(ColumnFilter.matchEq("a", "a1"));
	}

	@Test
	public void test_twiceSame() {
		ISliceFilter filterAllAndA =
				FilterBuilder.or(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("a", "a1")).optimize();

		Assertions.assertThat(filterAllAndA).isEqualTo(ColumnFilter.matchEq("a", "a1"));
	}

	@Test
	public void testOr_oneGrandTotal_TwoCustom() {
		ISliceFilter filterAllAndA = FilterBuilder
				.or(ISliceFilter.MATCH_NONE, ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("a", "a2"))
				.optimize();

		Assertions.assertThat(filterAllAndA).isEqualTo(ColumnFilter.matchIn("a", "a1", "a2"));
	}

	@Test
	public void testMultipleSameColumn_equalsAndIn() {
		ISliceFilter filterA1andInA12 =
				FilterBuilder.or(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchIn("a", "a1", "a2")).optimize();

		// At some point, this may be optimized into `ColumnFilter.isEqualTo("a", "a1")`
		Assertions.assertThat(filterA1andInA12).isEqualTo(filterA1andInA12);
	}

	@Test
	public void testMultipleSameColumn_InAndIn() {
		ISliceFilter filterA1andInA12 =
				FilterBuilder.or(ColumnFilter.matchIn("a", "a1", "a2"), ColumnFilter.matchIn("a", "a2", "a3"))
						.optimize();

		Assertions.assertThat(filterA1andInA12).isInstanceOfSatisfying(ColumnFilter.class, cf -> {
			Assertions.assertThat(cf.getColumn()).isEqualTo("a");
			Assertions.assertThat(cf.getValueMatcher()).isEqualTo(InMatcher.matchIn("a1", "a2", "a3"));
		});
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		ISliceFilter filter =
				FilterBuilder.or(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b2")).optimize();

		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		String asString = objectMapper.writeValueAsString(filter);
		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				{
				  "type" : "or",
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
	public void testJackson_matchNone() throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		objectMapper.registerModule(AdhocPublicJackson.makeModule());

		String asString = objectMapper.writeValueAsString(ISliceFilter.MATCH_NONE);
		Assertions.assertThat(asString).isEqualTo("""
				"matchNone"
								""".trim());

		ISliceFilter fromString = objectMapper.readValue(asString, ISliceFilter.class);

		Assertions.assertThat(fromString).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void testChained() {
		ISliceFilter a1Andb2 =
				FilterBuilder.or(ColumnFilter.matchEq("a", "a1"), ColumnFilter.matchEq("b", "b2")).optimize();
		ISliceFilter a1Andb2AndC3 = FilterBuilder.or(a1Andb2, ColumnFilter.matchEq("c", "c3")).optimize();

		Assertions.assertThat(a1Andb2AndC3).isInstanceOfSatisfying(OrFilter.class, orFilter -> {
			Assertions.assertThat(orFilter.getOperands()).hasSize(3);
		});
	}

	@Test
	public void testIncluded() {
		Assertions
				.assertThat(
						FilterBuilder.or(AndFilter.and(Map.of("a", "a1", "b", "b1")), AndFilter.and(Map.of("b", "b1")))
								.optimize())
				.isEqualTo(AndFilter.and(Map.of("b", "b1")));
	}

	@Test
	public void testOr_oneCommonColumn() {
		Assertions.assertThat(
				FilterBuilder.or(AndFilter.and(Map.of("a", "a1")), AndFilter.and(ImmutableMap.of("a", "a2", "b", "b1")))
						.optimize())
				.hasToString("a==a1|a==a2&b==b1");
	}

	// This test checks some optimization does not fails due to the large problem
	// Typically, some optimization may do a cartesian product and this should ensure such case are managed smoothly.
	// Happens typically with TableQueryOptimizerSinglePerAggregator which will to a large OR of the different table
	// queries
	@Test
	public void testOr_Large_onlyOrs() {
		List<ISliceFilter> operands = IntStream.range(0, 16)
				.mapToObj(i -> AndFilter.and(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
				.toList();

		Assertions.assertThat(FilterBuilder.or(operands).optimize())
				.hasToString(
						"a==a0&b==b0|a==a1&b==b1|a==a2&b==b2|a==a3&b==b3|a==a4&b==b4|a==a5&b==b5|a==a6&b==b6|a==a7&b==b7|a==a8&b==b8|a==a9&b==b9|a==a10&b==b10|a==a11&b==b11|a==a12&b==b12|a==a13&b==b13|a==a14&b==b14|a==a15&b==b15");
	}

	@Test
	public void testOr_Large_andMatchOnlyOne() {
		{
			List<ISliceFilter> operands = IntStream.range(0, 4)
					.mapToObj(i -> AndFilter.and(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
					.collect(Collectors.toCollection(ArrayList::new));

			operands.add(AndFilter.and(ImmutableMap.of("a", "a" + 0)));

			Assertions.assertThat(FilterBuilder.or(operands).optimize())
					.hasToString("a==a0|a==a1&b==b1|a==a2&b==b2|a==a3&b==b3");
		}
		{
			List<ISliceFilter> operands = IntStream.range(0, 16)
					.mapToObj(i -> AndFilter.and(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
					.collect(Collectors.toCollection(ArrayList::new));

			operands.add(AndFilter.and(ImmutableMap.of("a", "a" + 0)));

			Assertions.assertThat(FilterBuilder.or(operands).optimize())
					.hasToString(
							"a==a0|a==a1&b==b1|a==a2&b==b2|a==a3&b==b3|a==a4&b==b4|a==a5&b==b5|a==a6&b==b6|a==a7&b==b7|a==a8&b==b8|a==a9&b==b9|a==a10&b==b10|a==a11&b==b11|a==a12&b==b12|a==a13&b==b13|a==a14&b==b14|a==a15&b==b15");
		}
	}

	@Test
	public void testOr_Huge() {
		List<ISliceFilter> operands = IntStream.range(0, 128)
				.mapToObj(i -> AndFilter.and(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
				.collect(Collectors.toCollection(ArrayList::new));

		Assertions.assertThat(FilterBuilder.or(operands).optimize())
				.hasToString(
						"OrFilter{size=128, #0=a==a0&b==b0, #1=a==a1&b==b1, #2=a==a2&b==b2, #3=a==a3&b==b3, #4=a==a4&b==b4, #5=a==a5&b==b5, #6=a==a6&b==b6, #7=a==a7&b==b7, #8=a==a8&b==b8, #9=a==a9&b==b9, #10=a==a10&b==b10, #11=a==a11&b==b11, #12=a==a12&b==b12, #13=a==a13&b==b13, #14=a==a14&b==b14, #15=a==a15&b==b15}");
	}

	@Test
	public void testOr_AndWithAnOrOfAnds_operandIsAlwaysTrueGivenAllOthers() {
		List<ISliceFilter> operands = new ArrayList<ISliceFilter>();

		operands.add(ColumnFilter.matchEq("a", "a1"));
		ISliceFilter inB = ColumnFilter.matchIn("b", "b1", "b2", "b3");
		operands.add(inB);
		ISliceFilter inC = ColumnFilter.matchIn("c", "c1", "c2", "c3");
		operands.add(inC);

		// This is always true given previous operands
		operands.add(OrFilter.builder()
				.or(ColumnFilter.matchEq("d", "d1"))
				.or(AndFilter.and(ImmutableMap.of("b", "b1", "c", "c1")))
				.build());

		Assertions.assertThat(FilterBuilder.and(operands).optimize())
				.hasToString("a==a1&b=in=(b1,b2,b3)&c=in=(c1,c2,c3)&(d==d1|b==b1&c==c1)");
	}

	@Test
	public void testOr_AndWithAnOrOfAnds_operandIsAlwaysTrueGivenAllOthers_not() {
		List<ISliceFilter> operands = new ArrayList<ISliceFilter>();

		operands.add(ColumnFilter.matchEq("a", "a1"));
		ISliceFilter inB = ColumnFilter.matchIn("b", "b1", "b2", "b3");
		operands.add(inB);
		ISliceFilter inC = ColumnFilter.matchIn("c", "c1", "c2", "c3");
		operands.add(inC);

		// This is always true given previous operands
		operands.add(OrFilter.builder()
				.or(ColumnFilter.matchEq("d", "d1"))
				.or(AndFilter.and(ColumnFilter.matchEq("b", "b1"), ColumnFilter.matchEq("c", "c1").negate()))
				.build());

		Assertions.assertThat(FilterBuilder.and(operands).optimize())
				.hasToString("a==a1&b=in=(b1,b2,b3)&c=in=(c1,c2,c3)&(d==d1|b==b1&c!=c1)");
	}

	@Test
	public void testOr_AndWithAnOrOfAnds_andLong() {
		List<ISliceFilter> operands = new ArrayList<ISliceFilter>();

		operands.add(ColumnFilter.matchEq("a", "a1"));
		ISliceFilter inB = ColumnFilter.matchIn("b", "b1", "b2", "b3");
		operands.add(inB);
		ISliceFilter inC = ColumnFilter.matchIn("c", "c1", "c2", "c3");
		operands.add(inC);

		operands.add(OrFilter.builder()
				.or(ColumnFilter.matchEq("d", "d1"))
				.or(ColumnFilter.matchEq("b", "b1").and(ColumnFilter.matchEq("c", "c1").negate()))
				.or(ColumnFilter.matchEq("b", "b1").and(ColumnFilter.matchEq("c", "c2").negate()))
				.or(ColumnFilter.matchEq("b", "b1").and(ColumnFilter.matchEq("c", "c3").negate()))
				// Following ORs are always true given inB and inC
				.or(ColumnFilter.matchEq("b", "b1").and(ColumnFilter.matchEq("c", "c4").negate()))
				.or(ColumnFilter.matchEq("b", "b1").and(ColumnFilter.matchEq("c", "c5").negate()))
				.or(ColumnFilter.matchEq("b", "b1").and(ColumnFilter.matchEq("c", "c6").negate()))
				.or(ColumnFilter.matchEq("b", "b1").and(ColumnFilter.matchEq("c", "c7").negate()))
				.or(ColumnFilter.matchEq("b", "b1").and(ColumnFilter.matchEq("c", "c8").negate()))
				.build());

		Assertions.assertThat(FilterBuilder.and(operands).optimize())
				.hasToString("a==a1&b=in=(b1,b2,b3)&c=in=(c1,c2,c3)&(d==d1|b==b1)");
	}

	@Test
	public void testOr_AndWithAnOrOfAnds_andLong_2() {
		// Typically happens with TableQueryOptimizer:

		// There is a cubeQueryStep for a given filter
		List<ISliceFilter> operandsSlice = new ArrayList<ISliceFilter>();
		// which is part of a large OR, enabling to answer multiple cubeQuerySteps
		// This holds the filter common to all cubeQuerySteps
		List<ISliceFilter> tableWhere = new ArrayList<ISliceFilter>();
		// This holds the list of filters, each associated to a cubeQueryStep
		List<ISliceFilter> tableOr = new ArrayList<ISliceFilter>();

		// There is a common simple filter
		operandsSlice.add(ColumnFilter.matchEq("a", "a1"));
		tableWhere.add(ColumnFilter.matchEq("a", "a1"));

		// The OR has other cubeQueryStep referring to unrelated columns
		tableOr.add(ColumnFilter.matchEq("d", "d1"));

		// There is two common IN
		ISliceFilter inB = ColumnFilter.matchIn("b", "b1", "b2", "b3");
		operandsSlice.add(inB);
		ISliceFilter inC = ColumnFilter.matchIn("c", "c1", "c2", "c3");
		operandsSlice.add(inC);

		// current cubeQueryStep has a filter to a column specific to it
		operandsSlice.add(ColumnFilter.matchEq("e", "e1"));
		tableOr.add(FilterBuilder.and(inB, inC, ColumnFilter.matchEq("e", "e1")).combine());

		ISliceFilter cubeQueryStep = FilterBuilder.and(operandsSlice).combine();
		ISliceFilter tableQueryStep = FilterBuilder.and(tableWhere)
				.filter(
						// This combines, as it may be very wide and prone to cartesianProduct explosion
						FilterBuilder.or(tableOr).combine())
				.combine();

		// Check AND between the wide tableQuery and the cubeQueryStep gives
		ISliceFilter combined = FilterBuilder.and(tableQueryStep, cubeQueryStep).optimize();

		Assertions.assertThat(combined).hasToString("a==a1&e==e1&b=in=(b1,b2,b3)&c=in=(c1,c2,c3)");
	}

	@Test
	public void testOr_stripInducedByAllOthers() {
		// d==d1|b=in=(b1,b2,b3)&c=in=(c1,c2,c3)&e==e1, b=in=(b1,b2,b3), c=in=(c1,c2,c3)

		ISliceFilter output = FilterBuilder.and()
				.filter(ColumnFilter.matchEq("a", "a1"))
				.filter(ColumnFilter.matchIn("b", "b1", "b2", "b3"))
				.filter(ColumnFilter.matchIn("c", "c1", "c2", "c3"))
				// The following OR has an unrelated clause, and a clause which is implied by the other AND operands
				.filter(FilterBuilder
						.or(ColumnFilter.matchEq("e", "e1"),
								FilterBuilder
										.and(ColumnFilter.matchIn("b", "b1", "b2", "b3"),
												ColumnFilter.matchIn("c", "c1", "c2", "c3"))
										.optimize())
						.optimize())
				.optimize();

		Assertions.assertThat(output).hasToString("a==a1&b=in=(b1,b2,b3)&c=in=(c1,c2,c3)");
	}

	@Test
	public void testOr_AndNotOr_22() {
		// make sure this case is optimized without cartesianProductAndOr
		FilterOptimizer optimizer = FilterOptimizer.builder().withCartesianProductsAndOr(false).build();

		// `d!=d1&(d==d1|e!=e1)`
		ISliceFilter output = FilterBuilder.builder()
				.andElseOr(Type.AND)
				.build()
				.filter(FilterBuilder.and(ColumnFilter.matchEq("d", "d1")).combine().negate())
				.filter(FilterBuilder.and(ColumnFilter.notEq("d", "d1"), ColumnFilter.matchEq("e", "e1"))
						.combine()
						.negate())
				.optimize(optimizer);

		Assertions.assertThat(output).hasToString("d!=d1&e!=e1");
	}

	@Test
	public void testOr_AndNotOr() {
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
				.filter(FilterBuilder.and(ColumnFilter.notEq("d", "d1"), ColumnFilter.matchEq("e", "e1"), notLikeA1)
						.combine()
						.negate())
				.optimize();

		Assertions.assertThat(output)
				.hasToString(
						"b=out=(b1,b2,b3,b4)&c=out=(c1,c2)&d=out=(d1,d2)&e!=e1&a does NOT match `LikeMatcher(pattern=a1)`");
	}

	@Test
	public void testOr_AndNotOr_2() {
		ISliceFilter output = FilterBuilder.and()
				.filter(ColumnFilter.matchEq("b", "b1"))
				.filter(ColumnFilter.notIn("c", "c1", "c2"))
				.filter(ColumnFilter.matchEq("d", "d2"))

				.filter(FilterBuilder
						.and(ColumnFilter.notIn("c", "c1", "c2", "c3", "c4"),
								ColumnFilter.notIn("f", "f1", "f2"),

								FilterBuilder.or()
										.filter(FilterBuilder
												.and(ColumnFilter.notIn("d", "d2", "d3"),
														ColumnFilter.notLike("h", "h1"))
												.combine())
										.filter(FilterBuilder
												.and(ColumnFilter.notIn("d", "d2", "d3"),
														ColumnFilter.matchLike("g", "g1"))
												.combine())
										.filter(FilterBuilder
												.and(ColumnFilter.matchEq("d", "d3"), ColumnFilter.matchLike("g", "g1"))
												.combine())
										.filter(FilterBuilder
												.and(ColumnFilter.notEq("d", "d2"), ColumnFilter.matchLike("g", "g1"))
												.combine())

										.combine(),

								ColumnFilter.notLike("g", "g1"))
						.combine()
						.negate())
				.optimize();

		Assertions.assertThat(output).hasToString("b==b1&c=out=(c1,c2)&d==d2");
	}

	@Test
	public void testOrFilter_fromMap_matchAll() {
		Assertions.assertThat(OrFilter.or(Map.of("c", IValueMatcher.MATCH_ALL))).hasToString("matchAll");

		Assertions.assertThat(OrFilter.or(Map.of("c", IValueMatcher.MATCH_ALL, "d", "d1"))).hasToString("matchAll");

		Assertions.assertThat(OrFilter.or(Map.of("d", "d1", "e", IValueMatcher.MATCH_NONE))).hasToString("d==d1");
		Assertions
				.assertThat(
						OrFilter.or(Map.of("d", "d1", "e", IValueMatcher.MATCH_NONE, "f", IValueMatcher.MATCH_NONE)))
				.hasToString("d==d1");
	}

	@Test
	public void testOrFilter_cartesianProductToAndIn_detectUselessCombination() {
		ISliceFilter a_b =
				FilterBuilder.and(ColumnFilter.matchIn("a", "a1", "a2"), ColumnFilter.matchIn("b", "b1", "b2"))
						.optimize();
		ISliceFilter abc = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b1", "c", "c1"));

		// the `abc` constrains is swallowed by the `ab` contains given the OR
		// The output must still be an AND or IN, and not an expanded cartesianProduct
		Assertions.assertThat(FilterBuilder.or(a_b, abc).optimize()).hasToString("a=in=(a1,a2)&b=in=(b1,b2)");
	}

	@Test
	public void testOrFilter_cartesianProductToAndIn_line() {
		ISliceFilter a1b1 = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b1"));
		ISliceFilter a1b2 = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b2"));
		ISliceFilter a1b3 = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b3"));

		Assertions.assertThat(FilterBuilder.or(a1b1, a1b2, a1b3).optimize()).hasToString("a==a1&b=in=(b1,b2,b3)");
	}

	@Test
	public void testOrFilter_cartesianProductToAndIn_line_huge() {
		List<ISliceFilter> ands =
				IntStream.range(0, 128).mapToObj(i -> AndFilter.and(ImmutableMap.of("a", "a1", "b", "b" + i))).toList();

		FilterBuilder combined = FilterBuilder.builder().andElseOr(Type.OR).build();

		Assertions.assertThat(combined.filters(ands).optimize(optimizer))
				.hasToString(
						"a==a1&b=in=(b0,b1,b2,b3,b4,b5,b6,b7,b8,b9,b10,b11,b12,b13,b14,b15, and 112 more entries)");

		Assertions.assertThat(nbSkip).hasValue(0);
	}

	// TODO We do not manage noise yet
	@Test
	public void testOrFilter_cartesianProductToAndIn_line_withNoise() {
		ISliceFilter a1b1 = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b1"));
		ISliceFilter a1b2 = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b2"));
		ISliceFilter a1b3 = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b3"));
		ISliceFilter a2b4 = AndFilter.and(ImmutableMap.of("a", "a2", "b", "b4"));

		Assertions.assertThat(FilterBuilder.or(a1b1, a1b2, a1b3, a2b4).optimize())
				// .hasToString("a==a1&b=in=(b1,b2,b3)|a==a2&b==b4")
				.hasToString("a==a1&b==b1|a==a1&b==b2|a==a1&b==b3|a==a2&b==b4");
	}

	// TODO We do not guess cartesianProducts yet
	@Test
	public void testOrFilter_cartesianProductToAndIn_square() {
		ISliceFilter a1b1 = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b1"));
		ISliceFilter a1b2 = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b2"));
		ISliceFilter a2b1 = AndFilter.and(ImmutableMap.of("a", "a2", "b", "b1"));
		ISliceFilter a2b2 = AndFilter.and(ImmutableMap.of("a", "a2", "b", "b2"));

		Assertions.assertThat(FilterBuilder.or(a1b1, a1b2, a2b1, a2b2).optimize())
				.hasToString("a==a1&b==b1|a==a1&b==b2|a==a2&b==b1|a==a2&b==b2")
		// .hasToString("a=in=(a1,a2)&b=in=(b1,b2)")
		;
	}

	// TODO We do not guess cartesianProducts yet
	@Test
	public void testOrFilter_cartesianProductToAndIn_square_withHole() {
		ISliceFilter a1b1 = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b1"));
		ISliceFilter a1b2 = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b2"));
		ISliceFilter a1b3 = AndFilter.and(ImmutableMap.of("a", "a1", "b", "b3"));
		ISliceFilter a2b1 = AndFilter.and(ImmutableMap.of("a", "a2", "b", "b1"));
		ISliceFilter a2b2 = AndFilter.and(ImmutableMap.of("a", "a2", "b", "b2"));
		ISliceFilter a2b3 = AndFilter.and(ImmutableMap.of("a", "a2", "b", "b3"));
		ISliceFilter a3b1 = AndFilter.and(ImmutableMap.of("a", "a3", "b", "b1"));
		ISliceFilter a3b2 = AndFilter.and(ImmutableMap.of("a", "a3", "b", "b2"));

		Assertions.assertThat(FilterBuilder.or(a1b1, a1b2, a1b3, a2b1, a2b2, a2b3, a3b1, a3b2).optimize())
				.hasToString(
						"a==a1&b==b1|a==a1&b==b2|a==a1&b==b3|a==a2&b==b1|a==a2&b==b2|a==a2&b==b3|a==a3&b==b1|a==a3&b==b2")
		// .hasToString("a=in=(a1,a2,3)&b=in=(b1,b2,3)&!(a==a3&b==b3)")
		;
	}

	// Turns `a|a&b|a&b&c|...` into `a`, without failing on a cartesianProductLimit
	@Test
	public void testOr_deepChainOfStricterClauses() {
		List<ISliceFilter> ands = IntStream.range(1, 256)
				.<ISliceFilter>mapToObj(i -> AndFilter.and(IntStream.range(0, i)
						.<Integer>mapToObj(j -> j)
						.collect(Collectors.toMap(j -> Integer.toString(j), j -> "v"))))
				.toList();

		FilterBuilder combined = FilterBuilder.builder().andElseOr(Type.OR).build();

		Assertions.assertThat(combined.filters(ands).optimize(optimizer)).hasToString("0==v");
		Assertions.assertThat(nbSkip).hasValue(0);
	}

}
