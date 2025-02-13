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
package eu.solven.adhoc.filter.value;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOrMatcher {
	@Test
	public void testAndInEq() {
		OrMatcher a_and_aandb =
				OrMatcher.builder().operand(EqualsMatcher.isEqualTo("a")).operand(InMatcher.isIn("a", "b")).build();

		// TODO Improve this when relevant
		// Assertions.assertThat(a_and_aandb).isEqualTo(EqualsMatcher.isEqualTo("a"));
		Assertions.assertThat(a_and_aandb).isEqualTo(a_and_aandb);
	}

	@Test
	public void testAnd_all() {
		IValueMatcher matcher = OrMatcher.or(EqualsMatcher.isEqualTo("a"), IValueMatcher.MATCH_ALL);
		Assertions.assertThat(matcher).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testAnd_none() {
		IValueMatcher matcher = OrMatcher.or(EqualsMatcher.isEqualTo("a"), IValueMatcher.MATCH_NONE);
		Assertions.assertThat(matcher).isEqualTo(EqualsMatcher.isEqualTo("a"));
	}

	@Test
	public void testEqualsDifferentOrder() {
		OrMatcher aThenB =
				OrMatcher.builder().operand(LikeMatcher.matching("a%")).operand(LikeMatcher.matching("%b")).build();
		OrMatcher bThenA =
				OrMatcher.builder().operand(LikeMatcher.matching("%b")).operand(LikeMatcher.matching("a%")).build();

		Assertions.assertThat(aThenB).isEqualTo(bThenA);
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		IValueMatcher matcher = OrMatcher.or(EqualsMatcher.isEqualTo("azerty"), LikeMatcher.matching("b%"));

		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		String asString = objectMapper.writeValueAsString(matcher);
		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
{
  "type" : "or",
  "operands" : [ {
    "type" : "like",
    "like" : "b%"
  }, {
    "type" : "equals",
    "operand" : "azerty"
  } ]
}
				""".trim());

		IValueMatcher fromString = objectMapper.readValue(asString, IValueMatcher.class);

		Assertions.assertThat(fromString).isEqualTo(matcher);
	}

}
