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
package eu.solven.adhoc.query;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.solven.adhoc.query.cube.AdhocQuery;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.resource.AdhocJackson;

public class TestAdhocQueryBuilder {
	@Test
	public void testGrandTotal() {
		AdhocQuery q = AdhocQuery.builder().build();

		Assertions.assertThat(q.getFilter().isMatchAll()).isTrue();
		Assertions.assertThat(q.getGroupBy().isGrandTotal()).isTrue();
		Assertions.assertThat(q.getMeasures()).isEmpty();

		// Make sure the .toString returns actual values, and not the lambda toString
		Assertions.assertThat(q.toString()).doesNotContain("Lambda");
	}

	@Test
	public void testGrandTotal_filterAndEmpty() {
		AdhocQuery q = AdhocQuery.builder().andFilter(AndFilter.and(Map.of())).build();

		Assertions.assertThat(q.getFilter().isMatchAll()).isTrue();
		Assertions.assertThat(q.getGroupBy().isGrandTotal()).isTrue();
		Assertions.assertThat(q.getMeasures()).isEmpty();
	}

	@Test
	public void testEquals() {
		AdhocQuery q1 = AdhocQuery.builder().build();
		AdhocQuery q2 = AdhocQuery.builder().build();

		Assertions.assertThat(q1).isEqualTo(q2);
	}

	@Test
	public void testAddGroupBy() {
		AdhocQuery q1 = AdhocQuery.builder().groupByAlso("a", "b").groupByAlso("c", "d").build();

		Assertions.assertThat(q1.getGroupBy().getGroupedByColumns()).contains("a", "b", "c", "d");
	}

	@Test
	public void testResetGroupBy() {
		AdhocQuery q1 = AdhocQuery.builder().groupByAlso("a", "b").groupBy(GroupByColumns.named("c", "d")).build();

		Assertions.assertThat(q1.getGroupBy().getGroupedByColumns()).contains("c", "d");
	}

	@Test
	public void testCustomMarker() {
		Assertions.assertThat(AdhocQuery.builder().customMarker(null).build().getCustomMarker()).isNull();
		Assertions.assertThat(AdhocQuery.builder().customMarker(Optional.empty()).build().getCustomMarker()).isNull();

		Assertions.assertThat(AdhocQuery.builder().customMarker("someCustom").build().getCustomMarker())
				.isEqualTo("someCustom");
		Assertions.assertThat(AdhocQuery.builder().customMarker(Optional.of("someCustom")).build().getCustomMarker())
				.isEqualTo("someCustom");
	}

	@Test
	public void testUnionOptions() {
		AdhocQuery query = AdhocQuery.builder().option(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE).build();
		AdhocQuery edited = AdhocQuery.edit(query)
				// this test checks that `@Builder` `@Singular` does `.addAll` and not some `.clear`
				.options(Set.of(StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY))
				.build();

		Assertions.assertThat(edited.getOptions())
				.hasSize(2)
				.contains(StandardQueryOptions.EXCEPTIONS_AS_MEASURE_VALUE,
						StandardQueryOptions.UNKNOWN_MEASURES_ARE_EMPTY);
	}

	@Test
	public void testEdit() {
		AdhocQuery query = fullyCustomized();
		AdhocQuery edited = AdhocQuery.edit(query).build();

		Assertions.assertThat(edited).isEqualTo(query);
	}

	private AdhocQuery fullyCustomized() {
		return AdhocQuery.builder()
				.measure("k1.SUM")
				.groupByAlso("c1")
				.andFilter("c2", "v2")
				.customMarker("somethingCustom")
				.debug(true)
				.explain(true)
				.build();
	}

	@Test
	public void testJackson_empty() throws JsonProcessingException {
		AdhocQuery q1 = AdhocQuery.builder().build();

		ObjectMapper objectMapper = new ObjectMapper();
		String asString = objectMapper.writeValueAsString(q1);
		AdhocQuery fromString = objectMapper.readValue(asString, AdhocQuery.class);

		Assertions.assertThat(fromString).isEqualTo(q1);
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		AdhocQuery q1 = AdhocQuery.builder().measure("k1").andFilter("c1", "v1").groupByAlso("a").build();

		ObjectMapper objectMapper = new ObjectMapper();
		String asString = objectMapper.writeValueAsString(q1);
		AdhocQuery fromString = objectMapper.readValue(asString, AdhocQuery.class);

		Assertions.assertThat(fromString).isEqualTo(q1);
	}

	@Test
	public void testJackson_complex() throws JsonProcessingException {
		AdhocQuery query = fullyCustomized();

		ObjectMapper objectMapper = AdhocJackson.makeObjectMapper("json");
		String asString = objectMapper.writeValueAsString(query);
		AdhocQuery fromString = objectMapper.readValue(asString, AdhocQuery.class);

		Assertions.assertThat(fromString).isEqualTo(query);

		Assertions.assertThat(asString).isEqualToIgnoringNewLines("""
				{
				  "filter" : {
				    "type" : "column",
				    "column" : "c2",
				    "valueMatcher" : "v2",
				    "nullIfAbsent" : true
				  },
				  "groupBy" : {
				    "columns" : [ "c1" ]
				  },
				  "measures" : [ "k1.SUM" ],
				  "customMarker" : "somethingCustom",
				  "options" : [ "DEBUG", "EXPLAIN" ]
				}""");
	}
}
