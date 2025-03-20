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
package eu.solven.adhoc.measure.model;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import eu.solven.adhoc.data.tabular.TestMapBasedTabularView;
import eu.solven.adhoc.measure.ReferencedMeasure;

public class TestReferencedMeasure {
	@Test
	public void testJackson() throws JsonProcessingException {
		String asString =
				TestMapBasedTabularView.verifyJackson(ReferencedMeasure.class, ReferencedMeasure.ref("someMeasure"));

		Assertions.assertThat(asString).isEqualTo("""
				"someMeasure"
				""".strip());
	}

	@Disabled("Does not work as `type` is expected to be a minimal class, not a name like `ref`")
	@Test
	public void testJackson_rawFormat() throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		String asString = "{\"type\": \"ref\", \"ref\": \"someMeasure\"}";

		ReferencedMeasure fromString = objectMapper.readValue(asString, ReferencedMeasure.class);

		Assertions.assertThat(fromString).isEqualTo(ReferencedMeasure.ref("someMeasure"));
	}

	@Test
	public void testGetName() throws JsonProcessingException {
		ReferencedMeasure ref = ReferencedMeasure.ref("someMeasure");

		Assertions.assertThat(ref.getRef()).isEqualTo("someMeasure");
		Assertions.assertThat(ref.getName()).isEqualTo(ref.getRef());
	}
}
