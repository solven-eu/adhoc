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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.resource.AdhocPublicJackson;

public class TestOrFilter {
	// A short toString not to prevail is composition .toString
	@Test
	public void toString_empty() {
		ISliceFilter matchNone = ISliceFilter.MATCH_NONE;
		Assertions.assertThat(matchNone.toString()).isEqualTo("matchNone");

		Assertions.assertThat(matchNone.isMatchAll()).isFalse();
	}

	@Test
	public void toString_notEmpty() {
		ISliceFilter a1orb2 = OrFilter.or(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b2"));

		Assertions.assertThat(a1orb2.isMatchAll()).isFalse();
	}

	@Test
	public void toString_small() {
		List<ColumnFilter> filters = IntStream.range(0, 5)
				.mapToObj(
						i -> ColumnFilter.builder().column("k" + i).valueMatcher(LikeMatcher.matching("%" + i)).build())
				.collect(Collectors.toList());

		Assertions.assertThat(OrFilter.or(filters).toString())
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

		Assertions.assertThat(OrFilter.or(filters).toString())
				.contains("#0=k matches `LikeMatcher(pattern=%0)`, #1=k matches `LikeMatcher(pattern=%1)`")
				.doesNotContain("64")
				.hasSizeLessThan(1024);
	}

	@Test
	public void testAndFilters_twoGrandTotal() {
		ISliceFilter filterAllAndA = OrFilter.or(ISliceFilter.MATCH_ALL, ISliceFilter.MATCH_ALL);

		Assertions.assertThat(filterAllAndA).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testAndFilters_oneGrandTotal() {
		ISliceFilter filterAllAndA = OrFilter.or(ISliceFilter.MATCH_ALL, ColumnFilter.isEqualTo("a", "a1"));

		Assertions.assertThat(filterAllAndA).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testOr_oneGrandTotal_forced() {
		ISliceFilter filterAllAndA =
				OrFilter.builder().filter(ISliceFilter.MATCH_ALL).filter(ColumnFilter.isEqualTo("a", "a1")).build();

		// We forced an OrBuilder: It is not simplified into IAdhocFilter.MATCH_ALL but is is isMatchAll
		Assertions.assertThat(filterAllAndA.isMatchAll()).isTrue();
		Assertions.assertThat(filterAllAndA).isNotEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testOr_oneMatchNone() {
		ISliceFilter filterAllAndA = OrFilter.or(ISliceFilter.MATCH_NONE, ColumnFilter.isEqualTo("a", "a1"));

		Assertions.assertThat(filterAllAndA).isEqualTo(ColumnFilter.isEqualTo("a", "a1"));
	}

	@Test
	public void test_twiceSame() {
		ISliceFilter filterAllAndA = OrFilter.or(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("a", "a1"));

		Assertions.assertThat(filterAllAndA).isEqualTo(ColumnFilter.isEqualTo("a", "a1"));
	}

	@Test
	public void testOr_oneGrandTotal_TwoCustom() {
		ISliceFilter filterAllAndA = OrFilter
				.or(ISliceFilter.MATCH_NONE, ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("a", "a2"));

		Assertions.assertThat(filterAllAndA).isEqualTo(ColumnFilter.isIn("a", "a1", "a2"));
	}

	@Test
	public void testMultipleSameColumn_equalsAndIn() {
		ISliceFilter filterA1andInA12 =
				OrFilter.or(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isIn("a", "a1", "a2"));

		// At some point, this may be optimized into `ColumnFilter.isEqualTo("a", "a1")`
		Assertions.assertThat(filterA1andInA12).isEqualTo(filterA1andInA12);
	}

	@Test
	public void testMultipleSameColumn_InAndIn() {
		ISliceFilter filterA1andInA12 =
				OrFilter.or(ColumnFilter.isIn("a", "a1", "a2"), ColumnFilter.isIn("a", "a2", "a3"));

		Assertions.assertThat(filterA1andInA12).isInstanceOfSatisfying(ColumnFilter.class, cf -> {
			Assertions.assertThat(cf.getColumn()).isEqualTo("a");
			Assertions.assertThat(cf.getValueMatcher()).isEqualTo(InMatcher.isIn("a1", "a2", "a3"));
		});
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		ISliceFilter filter = OrFilter.or(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b2"));

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
		ISliceFilter a1Andb2 = OrFilter.or(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b2"));
		ISliceFilter a1Andb2AndC3 = OrFilter.or(a1Andb2, ColumnFilter.isEqualTo("c", "c3"));

		Assertions.assertThat(a1Andb2AndC3).isInstanceOfSatisfying(OrFilter.class, orFilter -> {
			Assertions.assertThat(orFilter.getOperands()).hasSize(3);
		});
	}

	@Test
	public void testIncluded() {
		Assertions
				.assertThat(OrFilter.or(AndFilter.and(Map.of("a", "a1", "b", "b1")), AndFilter.and(Map.of("b", "b1"))))
				.isEqualTo(AndFilter.and(Map.of("b", "b1")));
	}

	@Test
	public void testOr_oneCommonColumn() {
		Assertions
				.assertThat(OrFilter.or(AndFilter.and(Map.of("a", "a1")),
						AndFilter.and(ImmutableMap.of("a", "a2", "b", "b1"))))
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

		Assertions.assertThat(OrFilter.or(operands))
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

			Assertions.assertThat(OrFilter.or(operands)).hasToString("a==a0|a==a1&b==b1|a==a2&b==b2|a==a3&b==b3");
		}
		{
			List<ISliceFilter> operands = IntStream.range(0, 16)
					.mapToObj(i -> AndFilter.and(ImmutableMap.of("a", "a" + i, "b", "b" + i)))
					.collect(Collectors.toCollection(ArrayList::new));

			operands.add(AndFilter.and(ImmutableMap.of("a", "a" + 0)));

			Assertions.assertThat(OrFilter.or(operands))
					.hasToString(
							"a==a0|a==a1&b==b1|a==a2&b==b2|a==a3&b==b3|a==a4&b==b4|a==a5&b==b5|a==a6&b==b6|a==a7&b==b7|a==a8&b==b8|a==a9&b==b9|a==a10&b==b10|a==a11&b==b11|a==a12&b==b12|a==a13&b==b13|a==a14&b==b14|a==a15&b==b15");
		}
	}
}
