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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;

public class TestOrMatcher {

	@Test
	public void test_multipleEquals() {
		IValueMatcher a_and_aandb = OrMatcher.or(EqualsMatcher.equalTo("a"), EqualsMatcher.equalTo("b"));

		Assertions.assertThat(a_and_aandb).isInstanceOfSatisfying(InMatcher.class, inMatcher -> {
			Assertions.assertThat((Set) inMatcher.getOperands()).contains("a", "b");
		});
	}

	@Test
	public void testOr_InEq() {
		IValueMatcher a_or_bc = OrMatcher.or(EqualsMatcher.equalTo("a"), InMatcher.isIn("b", "c"));

		Assertions.assertThat(a_or_bc).isInstanceOfSatisfying(InMatcher.class, inMatcher -> {
			Assertions.assertThat((Set) inMatcher.getOperands()).contains("a", "b", "c");
		});
	}

	@Test
	public void testOr_InEq_overlap() {
		IValueMatcher a_or_ab = OrMatcher.or(EqualsMatcher.equalTo("a"), InMatcher.isIn("a", "b"));

		Assertions.assertThat(a_or_ab).isEqualTo(InMatcher.isIn("a", "b"));
	}

	@Test
	public void testOr_InIn() {
		IValueMatcher a_or_bc = OrMatcher.or(InMatcher.isIn("a", "b"), InMatcher.isIn("b", "c"));

		Assertions.assertThat(a_or_bc).isInstanceOfSatisfying(InMatcher.class, inMatcher -> {
			Assertions.assertThat((Set) inMatcher.getOperands()).contains("a", "b", "c");
		});
	}

	@Test
	public void testAnd_all() {
		IValueMatcher matcher = OrMatcher.or(EqualsMatcher.equalTo("a"), IValueMatcher.MATCH_ALL);
		Assertions.assertThat(matcher).isEqualTo(IValueMatcher.MATCH_ALL);
	}

	@Test
	public void testAnd_none() {
		IValueMatcher matcher = OrMatcher.or(EqualsMatcher.equalTo("a"), IValueMatcher.MATCH_NONE);
		Assertions.assertThat(matcher).isEqualTo(EqualsMatcher.equalTo("a"));
	}

	@Test
	public void testEqualsDifferentOrder() {
		OrMatcher aThenB =
				OrMatcher.builder().operand(LikeMatcher.matching("a%")).operand(LikeMatcher.matching("%b")).build();
		OrMatcher bThenA =
				OrMatcher.builder().operand(LikeMatcher.matching("%b")).operand(LikeMatcher.matching("a%")).build();

		Assertions.assertThat(aThenB).isEqualTo(bThenA).hasToString("LikeMatcher(pattern=a%)|LikeMatcher(pattern=%b)");
	}

	@Test
	public void testNotOrEquals_multiple() {
		Assertions.assertThat(NotMatcher.not(OrMatcher.or(EqualsMatcher.equalTo("foo"), EqualsMatcher.equalTo("bar"))))
				.isInstanceOfSatisfying(NotMatcher.class, notMatcher -> {
					Assertions.assertThat(notMatcher.getNegated())
							.isInstanceOfSatisfying(InMatcher.class, inMatcher -> {
								Assertions.assertThat((Set) inMatcher.getOperands())
										.containsExactlyInAnyOrder("foo", "bar");
							});
				});
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		IValueMatcher matcher = OrMatcher.or(EqualsMatcher.equalTo("azerty"), LikeMatcher.matching("b%"));

		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		String asString = objectMapper.writeValueAsString(matcher);
		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				{
				  "type" : "or",
				  "operands" : [ "azerty", {
				    "type" : "like",
				    "pattern" : "b%"
				  } ]
				}
																				""".trim());

		IValueMatcher fromString = objectMapper.readValue(asString, IValueMatcher.class);

		Assertions.assertThat(fromString).isEqualTo(matcher);
	}

}
