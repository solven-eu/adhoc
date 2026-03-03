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
package eu.solven.adhoc.query.filter.jackson;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.resource.AdhocPublicJackson;

public class TestSliceFilterJackson {

	final ObjectMapper mapper = new ObjectMapper().registerModule(AdhocPublicJackson.makeModule());

	// -----------------------------------------------------------------------
	// Serialization
	// -----------------------------------------------------------------------

	@Test
	public void serialize_matchAll() throws JsonProcessingException {
		String json = mapper.writeValueAsString(ISliceFilter.MATCH_ALL);
		// MATCH_ALL is serialized as the plain string "matchAll"
		Assertions.assertThat(json).isEqualTo("\"matchAll\"");
	}

	@Test
	public void serialize_matchNone() throws JsonProcessingException {
		String json = mapper.writeValueAsString(ISliceFilter.MATCH_NONE);
		// MATCH_NONE is serialized as the plain string "matchNone"
		Assertions.assertThat(json).isEqualTo("\"matchNone\"");
	}

	@Test
	public void serialize_columnFilter() throws JsonProcessingException {
		String json = mapper.writeValueAsString(ColumnFilter.matchEq("col", "v"));
		// Regular filter is serialized via the default bean serializer, not as a bare string
		Assertions.assertThat(json).contains("\"col\"").contains("\"v\"");
		Assertions.assertThat(json).doesNotStartWith("\"matchAll\"");
		Assertions.assertThat(json).doesNotStartWith("\"matchNone\"");
	}

	// -----------------------------------------------------------------------
	// Deserialization
	// -----------------------------------------------------------------------

	@Test
	public void deserialize_matchAll() throws JsonProcessingException {
		ISliceFilter filter = mapper.readValue("\"matchAll\"", ISliceFilter.class);
		Assertions.assertThat(filter).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void deserialize_matchAll_caseInsensitive() throws JsonProcessingException {
		// onText uses equalsIgnoreCase
		ISliceFilter filter = mapper.readValue("\"MATCHALL\"", ISliceFilter.class);
		Assertions.assertThat(filter).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void deserialize_matchNone() throws JsonProcessingException {
		ISliceFilter filter = mapper.readValue("\"matchNone\"", ISliceFilter.class);
		Assertions.assertThat(filter).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void deserialize_matchNone_caseInsensitive() throws JsonProcessingException {
		ISliceFilter filter = mapper.readValue("\"MATCHNONE\"", ISliceFilter.class);
		Assertions.assertThat(filter).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void deserialize_columnFilter() throws JsonProcessingException {
		// A regular JSON object (with type property) should deserialize into a ColumnFilter
		String json = mapper.writeValueAsString(ColumnFilter.matchEq("myCol", "myVal"));
		ISliceFilter filter = mapper.readValue(json, ISliceFilter.class);
		Assertions.assertThat(filter).isEqualTo(ColumnFilter.matchEq("myCol", "myVal"));
	}

	@Test
	public void deserialize_unknownText_throws() {
		Assertions.assertThatThrownBy(() -> mapper.readValue("\"unknownKeyword\"", ISliceFilter.class))
				.isInstanceOf(Exception.class);
	}

	// -----------------------------------------------------------------------
	// Round-trip
	// -----------------------------------------------------------------------

	@Test
	public void roundTrip_matchAll() throws JsonProcessingException {
		String json = mapper.writeValueAsString(ISliceFilter.MATCH_ALL);
		ISliceFilter restored = mapper.readValue(json, ISliceFilter.class);
		Assertions.assertThat(restored).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void roundTrip_matchNone() throws JsonProcessingException {
		String json = mapper.writeValueAsString(ISliceFilter.MATCH_NONE);
		ISliceFilter restored = mapper.readValue(json, ISliceFilter.class);
		Assertions.assertThat(restored).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void roundTrip_columnFilter() throws JsonProcessingException {
		ISliceFilter original = ColumnFilter.matchEq("k", "v");
		String json = mapper.writeValueAsString(original);
		ISliceFilter restored = mapper.readValue(json, ISliceFilter.class);
		Assertions.assertThat(restored).isEqualTo(original);
	}
}
