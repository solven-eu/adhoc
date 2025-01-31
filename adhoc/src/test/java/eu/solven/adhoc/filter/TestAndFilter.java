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
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;

public class TestAndFilter {
	// A short toString not to prevail is composition .toString
	@Test
	public void toString_grandTotal() {
		IAdhocFilter filter = IAdhocFilter.MATCH_ALL;

		Assertions.assertThat(filter.toString()).isEqualTo("matchAll");
		Assertions.assertThat(filter.isMatchAll()).isTrue();
	}

	@Test
	public void toString_huge() {
		List<ColumnFilter> filters = IntStream.range(0, 256)
				.mapToObj(i -> ColumnFilter.builder().column("k").matching(i).build())
				.collect(Collectors.toList());

		Assertions.assertThat(AndFilter.and(filters).toString())
				.contains("#0=k=0", "#1=k=1")
				.doesNotContain("7")
				.hasSizeLessThan(512);
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
	public void testAndFilters_oneGrandTotal_TwoCustom() {
		IAdhocFilter filterAllAndA = AndFilter
				.and(IAdhocFilter.MATCH_ALL, ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("a", "a2"));

		Assertions.assertThat(filterAllAndA).isInstanceOfSatisfying(AndFilter.class, andF -> {
			Assertions.assertThat(andF.getOperands())
					.hasSize(2)
					.contains(ColumnFilter.isEqualTo("a", "a1"), ColumnFilter.isEqualTo("a", "a2"));
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
				    "valueMatcher" : {
				      "type" : "equals",
				      "operand" : "a1"
				    },
				    "nullIfAbsent" : true
				  }, {
				    "type" : "column",
				    "column" : "b",
				    "valueMatcher" : {
				      "type" : "equals",
				      "operand" : "b2"
				    },
				    "nullIfAbsent" : true
				  } ]
				}
				""".trim());

		IAdhocFilter fromString = objectMapper.readValue(asString, IAdhocFilter.class);

		Assertions.assertThat(fromString).isEqualTo(filter);
	}

	@Test
	public void testChained() {
		IAdhocFilter a1 = AndFilter.and(ColumnFilter.isEqualTo("a", "a1"));
		IAdhocFilter a1Andb2 = AndFilter.and(a1, ColumnFilter.isEqualTo("b", "b2"));
		IAdhocFilter a1Andb2AndC3 = AndFilter.and(a1Andb2, ColumnFilter.isEqualTo("c", "c3"));

		Assertions.assertThat(a1Andb2AndC3).isInstanceOfSatisfying(AndFilter.class, andFilter -> {
			Assertions.assertThat(andFilter.getOperands()).hasSize(3);
		});
	}
}
