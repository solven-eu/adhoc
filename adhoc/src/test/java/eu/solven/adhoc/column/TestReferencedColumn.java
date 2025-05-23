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
package eu.solven.adhoc.column;

import java.util.Arrays;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import eu.solven.adhoc.data.tabular.TestMapBasedTabularView;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

public class TestReferencedColumn {
	@Test
	public void testJackson() throws JsonProcessingException {
		String asString = TestMapBasedTabularView.verifyJackson(IAdhocColumn.class, ReferencedColumn.ref("someColumn"));

		Assertions.assertThat(asString).isEqualTo("""
				"someColumn"
				""".strip());
	}

	@Test
	public void testJackson_rawFormat() throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		String asString = "{\"type\": \".ReferencedColumn\", \"name\": \"someColumn\"}";

		ReferencedColumn fromString = objectMapper.readValue(asString, ReferencedColumn.class);

		Assertions.assertThat(fromString).isEqualTo(ReferencedColumn.ref("someColumn"));
	}

	@Disabled("Irrelevant as the type is converted by default into a String, which is not convertible into a Map")
	@Test
	public void testJackson_asMap() throws JsonProcessingException {
		ObjectMapper om = new ObjectMapper();

		ReferencedColumn initial = ReferencedColumn.ref("someColumn");
		Map asMap = om.convertValue(initial, Map.class);

		IAdhocColumn fromMap = om.convertValue(asMap, IAdhocColumn.class);

		Assertions.assertThat(fromMap).isEqualTo(initial);
	}

	// https://github.com/FasterXML/jackson-databind/issues/5030
	@Test
	public void testJackson_asMap_wrappedInGroupByColumns() throws JsonProcessingException {
		ObjectMapper om = new ObjectMapper();

		IAdhocGroupBy initial = GroupByColumns.of(ReferencedColumn.ref("someColumn"));
		Map asMap = om.convertValue(initial, Map.class);

		Assertions.assertThat(asMap).hasSize(1).containsEntry("columns", Arrays.asList("someColumn"));

		IAdhocGroupBy fromMap = om.convertValue(asMap, IAdhocGroupBy.class);

		Assertions.assertThat(fromMap).isEqualTo(initial);
	}

	@Value
	@Builder
	@Jacksonized
	public static class HasColumn {
		IAdhocColumn c;
	}

	// https://github.com/FasterXML/jackson-databind/issues/5030
	@Test
	public void testJackson_asMap_wrappedInPojo() throws JsonProcessingException {
		ObjectMapper om = new ObjectMapper();

		HasColumn initial = HasColumn.builder().c(ReferencedColumn.ref("someColumn")).build();
		Map asMap = om.convertValue(initial, Map.class);

		Assertions.assertThat(asMap).hasSize(1).containsEntry("c", "someColumn");

		HasColumn fromMap = om.convertValue(asMap, HasColumn.class);

		Assertions.assertThat(fromMap).isEqualTo(initial);
	}

	@Disabled("Irrelevant as Map does not hint the embedded type")
	@Test
	public void testJackson_asMap_wrappedInMap() throws JsonProcessingException {
		ObjectMapper om = new ObjectMapper();

		Map<String, ?> initial = Map.of("k", Arrays.asList(ReferencedColumn.ref("someColumn")));
		Map asMap = om.convertValue(initial, Map.class);

		Assertions.assertThat(asMap).hasSize(1).containsEntry("k", Arrays.asList("someColumn"));

		Map fromMap = om.convertValue(asMap, Map.class);

		Assertions.assertThat(fromMap).isEqualTo(initial);
	}

}
