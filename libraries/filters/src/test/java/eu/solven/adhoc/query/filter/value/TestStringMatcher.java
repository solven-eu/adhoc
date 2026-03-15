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
package eu.solven.adhoc.query.filter.value;

import java.time.LocalDate;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

public class TestStringMatcher {
	LocalDate today = LocalDate.now();

	@Test
	public void testSimple() {
		IValueMatcher equalsMatcher = StringMatcher.hasToString(today);

		Assertions.assertThat(equalsMatcher.match(today)).isEqualTo(true);
		Assertions.assertThat(equalsMatcher.match(today.toString())).isEqualTo(true);

		Assertions.assertThat(equalsMatcher.match(today.plusDays(1))).isEqualTo(false);
		Assertions.assertThat(equalsMatcher.match("foo")).isEqualTo(false);

		Assertions.assertThat(equalsMatcher.match(null)).isEqualTo(false);
	}

	@Test
	public void testJackson() {
		IValueMatcher stringMatcher = StringMatcher.hasToString(today);

		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		ObjectMapper objectMapper = JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).build();

		String asString = objectMapper.writeValueAsString(stringMatcher);
		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				{
				  "type" : "string",
				  "string" : "%s"
				}""".formatted(today));

		IValueMatcher fromString = objectMapper.readValue(asString, IValueMatcher.class);

		Assertions.assertThat(fromString).isEqualTo(stringMatcher);
	}

}
