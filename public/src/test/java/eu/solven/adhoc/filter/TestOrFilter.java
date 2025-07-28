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
package eu.solven.adhoc.filter;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.resource.AdhocPublicJackson;

public class TestOrFilter {
	// A short toString not to prevail is composition .toString
	@Test
	public void toString_empty() {
		IAdhocFilter matchNone = IAdhocFilter.MATCH_NONE;
		Assertions.assertThat(matchNone.toString()).isEqualTo("matchNone");

		Assertions.assertThat(matchNone.isMatchAll()).isFalse();
	}

	@Test
	public void toString_notEmpty() {
		IAdhocFilter a1orb2 = OrFilter.or(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b2"));

		Assertions.assertThat(a1orb2.isMatchAll()).isFalse();
	}

	@Test
	public void toString_small() {
		List<ColumnFilter> filters = IntStream.range(0, 5)
				.mapToObj(i -> ColumnFilter.builder().column("k" + i).matching(i).build())
				.collect(Collectors.toList());

		Assertions.assertThat(OrFilter.or(filters).toString()).isEqualTo("k0==0|k1==1|k2==2|k3==3|k4==4");
	}

	@Test
	public void toString_huge() {
		List<ColumnFilter> filters = IntStream.range(0, 256)
				.mapToObj(i -> ColumnFilter.builder().column("k").matching(i).build())
				.collect(Collectors.toList());

		Assertions.assertThat(OrFilter.or(filters).toString())
				.contains("#0=k==0", "#1=k==1")
				.doesNotContain("7")
				.hasSizeLessThan(512);
	}

	@Test
	public void testAndFilters_twoGrandTotal() {
		IAdhocFilter filterAllAndA = OrFilter.or(IAdhocFilter.MATCH_ALL, IAdhocFilter.MATCH_ALL);

		Assertions.assertThat(filterAllAndA).isEqualTo(IAdhocFilter.MATCH_ALL);
	}

	@Test
	public void testAndFilters_oneGrandTotal() {
		IAdhocFilter filterAllAndA = OrFilter.or(IAdhocFilter.MATCH_ALL, ColumnFilter.isEqualTo("a", "a1"));

		Assertions.assertThat(filterAllAndA).isEqualTo(IAdhocFilter.MATCH_ALL);
	}

	@Test
	public void testOr_oneGrandTotal_forced() {
		IAdhocFilter filterAllAndA =
				OrFilter.builder().filter(IAdhocFilter.MATCH_ALL).filter(ColumnFilter.isEqualTo("a", "a1")).build();

		// We forced an OrBuilder: It is not simplified into IAdhocFilter.MATCH_ALL but is is isMatchAll
		Assertions.assertThat(filterAllAndA.isMatchAll()).isTrue();
		Assertions.assertThat(filterAllAndA).isNotEqualTo(IAdhocFilter.MATCH_ALL);
	}

	@Test
	public void testOr_oneMatchNone() {
		IAdhocFilter filterAllAndA = OrFilter.or(IAdhocFilter.MATCH_NONE, ColumnFilter.isEqualTo("a", "a1"));

		Assertions.assertThat(filterAllAndA).isEqualTo(ColumnFilter.isEqualTo("a", "a1"));
	}

	@Test
	public void testOr_oneGrandTotal_TwoCustom() {
		IAdhocFilter filterAllAndA = OrFilter
				.or(IAdhocFilter.MATCH_NONE, ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("a", "a2"));

		Assertions.assertThat(filterAllAndA).isInstanceOfSatisfying(OrFilter.class, andF -> {
			Assertions.assertThat(andF.getOperands())
					.hasSize(2)
					.contains(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("a", "a2"));
		});
	}

	@Test
	public void testMultipleSameColumn_equalsAndIn() {
		IAdhocFilter filterA1andInA12 =
				OrFilter.or(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isIn("a", "a1", "a2"));

		// At some point, this may be optimized into `ColumnFilter.isEqualTo("a", "a1")`
		Assertions.assertThat(filterA1andInA12).isEqualTo(filterA1andInA12);
	}

	@Disabled("TODO Implement this optimization")
	@Test
	public void testMultipleSameColumn_InAndIn() {
		IAdhocFilter filterA1andInA12 =
				OrFilter.or(ColumnFilter.isIn("a", "a1", "a2"), ColumnFilter.isIn("a", "a2", "a3"));

		Assertions.assertThat(filterA1andInA12).isInstanceOfSatisfying(ColumnFilter.class, cf -> {
			Assertions.assertThat(cf.getColumn()).isEqualTo("a");
			Assertions.assertThat(cf.getValueMatcher()).isEqualTo(InMatcher.isIn("a1", "a2", "s3"));
		});
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		IAdhocFilter filter = OrFilter.or(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b2"));

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

		IAdhocFilter fromString = objectMapper.readValue(asString, IAdhocFilter.class);

		Assertions.assertThat(fromString).isEqualTo(filter);
	}

	@Test
	public void testJackson_matchNone() throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		objectMapper.registerModule(AdhocPublicJackson.makeModule());

		String asString = objectMapper.writeValueAsString(IAdhocFilter.MATCH_NONE);
		Assertions.assertThat(asString).isEqualTo("""
				"matchNone"
								""".trim());

		IAdhocFilter fromString = objectMapper.readValue(asString, IAdhocFilter.class);

		Assertions.assertThat(fromString).isEqualTo(IAdhocFilter.MATCH_NONE);
	}

	@Test
	public void testChained() {
		IAdhocFilter a1Andb2 = OrFilter.or(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("b", "b2"));
		IAdhocFilter a1Andb2AndC3 = OrFilter.or(a1Andb2, ColumnFilter.isEqualTo("c", "c3"));

		Assertions.assertThat(a1Andb2AndC3).isInstanceOfSatisfying(OrFilter.class, orFilter -> {
			Assertions.assertThat(orFilter.getOperands()).hasSize(3);
		});
	}
}
