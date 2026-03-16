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
package eu.solven.adhoc.filter.jackson;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.AdhocPublicJackson;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import tools.jackson.databind.ObjectMapper;

public class TestSliceFilterJackson {

	final ObjectMapper mapper = AdhocPublicJackson.makeObjectMapper();

	// -----------------------------------------------------------------------
	// Serialization
	// -----------------------------------------------------------------------

	@Test
	public void serialize_matchAll() {
		String json = mapper.writeValueAsString(ISliceFilter.MATCH_ALL);
		// MATCH_ALL is serialized as the plain string "matchAll"
		Assertions.assertThat(json).isEqualTo("\"matchAll\"");
	}

	@Test
	public void serialize_matchNone() {
		String json = mapper.writeValueAsString(ISliceFilter.MATCH_NONE);
		// MATCH_NONE is serialized as the plain string "matchNone"
		Assertions.assertThat(json).isEqualTo("\"matchNone\"");
	}

	@Test
	public void serialize_columnFilter() {
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
	public void deserialize_matchAll() {
		ISliceFilter filter = mapper.readValue("\"matchAll\"", ISliceFilter.class);
		Assertions.assertThat(filter).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void deserialize_matchAll_caseInsensitive() {
		// onText uses equalsIgnoreCase
		ISliceFilter filter = mapper.readValue("\"MATCHALL\"", ISliceFilter.class);
		Assertions.assertThat(filter).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void deserialize_matchNone() {
		ISliceFilter filter = mapper.readValue("\"matchNone\"", ISliceFilter.class);
		Assertions.assertThat(filter).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void deserialize_matchNone_caseInsensitive() {
		ISliceFilter filter = mapper.readValue("\"MATCHNONE\"", ISliceFilter.class);
		Assertions.assertThat(filter).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void deserialize_columnFilter() {
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
	public void roundTrip_matchAll() {
		String json = mapper.writeValueAsString(ISliceFilter.MATCH_ALL);
		ISliceFilter restored = mapper.readValue(json, ISliceFilter.class);
		Assertions.assertThat(restored).isEqualTo(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void roundTrip_matchNone() {
		String json = mapper.writeValueAsString(ISliceFilter.MATCH_NONE);
		ISliceFilter restored = mapper.readValue(json, ISliceFilter.class);
		Assertions.assertThat(restored).isEqualTo(ISliceFilter.MATCH_NONE);
	}

	@Test
	public void roundTrip_columnFilter() {
		ISliceFilter original = ColumnFilter.matchEq("k", "v");
		String json = mapper.writeValueAsString(original);
		ISliceFilter restored = mapper.readValue(json, ISliceFilter.class);
		Assertions.assertThat(restored).isEqualTo(original);
	}
}
