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

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.AdhocPublicJackson;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

public class TestAndMatcher {
	@Test
	public void testAndInEq() {
		IValueMatcher a_and_aandb = AndMatcher.and(EqualsMatcher.matchEq("a"), InMatcher.matchIn("a", "b"));

		Assertions.assertThat(a_and_aandb).isEqualTo(EqualsMatcher.matchEq("a"));
	}

	@Test
	public void testAnd_all() {
		IValueMatcher matcher = AndMatcher.and(EqualsMatcher.matchEq("a"), IValueMatcher.MATCH_ALL);
		Assertions.assertThat(matcher).isEqualTo(EqualsMatcher.matchEq("a"));
	}

	@Test
	public void testAnd_none() {
		IValueMatcher matcher = AndMatcher.and(EqualsMatcher.matchEq("a"), IValueMatcher.MATCH_NONE);
		Assertions.assertThat(matcher).isEqualTo(IValueMatcher.MATCH_NONE);
	}

	@Test
	public void testEqualsDifferentOrder() {
		AndMatcher aThenB =
				AndMatcher.builder().and(LikeMatcher.matching("a%")).and(LikeMatcher.matching("%b")).build();
		AndMatcher bThenA =
				AndMatcher.builder().and(LikeMatcher.matching("%b")).and(LikeMatcher.matching("a%")).build();

		Assertions.assertThat(aThenB).isEqualTo(bThenA);
	}

	@Test
	public void testAndEqualsLike() {
		Assertions.assertThat(AndMatcher.and(EqualsMatcher.matchEq("azerty"), LikeMatcher.matching("b%")))
				.isEqualTo(IValueMatcher.MATCH_NONE);
	}

	@Test
	public void testAndNotEquals_multiple() {
		Assertions
				.assertThat(AndMatcher.and(NotMatcher.not(EqualsMatcher.matchEq("foo")),
						NotMatcher.not(EqualsMatcher.matchEq("bar"))))
				.isInstanceOfSatisfying(NotMatcher.class, notMatcher -> {
					Assertions.assertThat(notMatcher.getNegated())
							.isInstanceOfSatisfying(InMatcher.class, inMatcher -> {
								Assertions.assertThat((Set) inMatcher.getOperands())
										.containsExactlyInAnyOrder("foo", "bar");
							});
				});
	}

	@Test
	public void testJackson() {
		IValueMatcher matcher =
				AndMatcher.and(NotMatcher.not(EqualsMatcher.matchEq("azerty")), LikeMatcher.matching("a%"));

		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		ObjectMapper objectMapper = JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).build();

		String asString = objectMapper.writeValueAsString(matcher);
		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				{
				  "type" : "and",
				  "operands" : [ {
				    "type" : "not",
				    "negated" : "azerty"
				  }, {
				    "type" : "like",
				    "pattern" : "a%"
				  } ]
				}""");

		IValueMatcher fromString = objectMapper.readValue(asString, IValueMatcher.class);

		Assertions.assertThat(fromString).isEqualTo(matcher);
	}

	@Test
	public void testJackson_matchAll() {
		IValueMatcher matcher = IValueMatcher.MATCH_ALL;

		ObjectMapper objectMapper = AdhocPublicJackson.makeObjectMapper();

		String asString = objectMapper.writeValueAsString(matcher);
		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				{
				  "type" : "and",
				  "operands" : [ ]
				}""");

		IValueMatcher fromString = objectMapper.readValue(asString, IValueMatcher.class);

		Assertions.assertThat(fromString).isEqualTo(matcher);
	}

	@Test
	public void testToString_or() {
		IValueMatcher matcher = AndMatcher.and(LikeMatcher.matching("a%"),
				OrMatcher.or(LikeMatcher.matching("%b%"), LikeMatcher.matching("%c%")));

		Assertions.assertThat(matcher).hasToString("LIKE 'a%'&(LIKE '%b%'|LIKE '%c%')");

	}

}
