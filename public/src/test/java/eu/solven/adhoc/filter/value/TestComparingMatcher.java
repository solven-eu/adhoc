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

import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;

public class TestComparingMatcher {
	@Test
	public void testJackson() throws JsonProcessingException {
		ComparingMatcher matcher = ComparingMatcher.builder().greaterThan(true).operand(123).build();

		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		String asString = objectMapper.writeValueAsString(matcher);
		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				{
				  "type" : "compare",
				  "operand" : 123,
				  "greaterThan" : true,
				  "matchIfEqual" : false,
				  "matchIfNull" : false
				}
								""".trim());

		IValueMatcher fromString = objectMapper.readValue(asString, IValueMatcher.class);

		Assertions.assertThat(fromString).isEqualTo(matcher);
	}

	@Test
	public void testDefaultFlags() {
		ComparingMatcher comparing = ComparingMatcher.builder().operand(123).build();

		// By default, it is `>`, not `<`
		Assertions.assertThat(comparing.isGreaterThan()).isFalse();
		// By default, it is `>`, not `>=`
		Assertions.assertThat(comparing.isMatchIfEqual()).isFalse();
		// By default, it does not match null
		Assertions.assertThat(comparing.isMatchIfNull()).isFalse();
	}

	@Test
	public void testComparing() {
		// >= 123
		ComparingMatcher comparing123 = ComparingMatcher.builder().greaterThan(true).operand(123).build();
		// >= 234
		ComparingMatcher comparing234 = ComparingMatcher.builder().greaterThan(true).operand(234).build();

		Assertions.assertThat(AndMatcher.and(comparing123, comparing234)).isEqualTo(comparing234);
		Assertions.assertThat(OrMatcher.or(comparing123, comparing234)).isEqualTo(comparing123);
	}

	@Test
	public void testComparing_and_greaterThan_Equals_compatible() {
		// >= 123
		ComparingMatcher comparing123 = ComparingMatcher.builder().greaterThan(true).operand(123).build();
		// == 234
		IValueMatcher equals234 = EqualsMatcher.isEqualTo(234);

		Assertions.assertThat(AndMatcher.and(comparing123, equals234)).isEqualTo(equals234);
		Assertions.assertThat(OrMatcher.or(comparing123, equals234)).isEqualTo(comparing123);
	}

	@Test
	public void testComparing_and_greaterThan_Equals_incompatible() {
		// == 123
		IValueMatcher equals123 = EqualsMatcher.isEqualTo(123);
		// >= 234
		ComparingMatcher comparing234 = ComparingMatcher.builder().greaterThan(true).operand(234).build();

		Assertions.assertThat(AndMatcher.and(comparing234, equals123)).isEqualTo(IValueMatcher.MATCH_NONE);
		Assertions.assertThat(OrMatcher.or(comparing234, equals123))
				.isInstanceOfSatisfying(OrMatcher.class, orMatcher -> {
					Assertions.assertThat(orMatcher.getOperands()).hasSize(2).contains(equals123, comparing234);
				});
	}

	@Test
	public void testComparing_and_greaterThan_In_partiallyCompatible() {
		// >= 234
		ComparingMatcher comparing234 = ComparingMatcher.builder().greaterThan(true).operand(234).build();
		// in (123,345)
		IValueMatcher in123_345 = InMatcher.isIn(123, 345);

		Assertions.assertThat(AndMatcher.and(comparing234, in123_345)).isEqualTo(InMatcher.isIn(Set.of(345)));
		Assertions.assertThat(OrMatcher.or(comparing234, in123_345))
				.isInstanceOfSatisfying(OrMatcher.class, orMatcher -> {
					Assertions.assertThat(orMatcher.getOperands())
							.hasSize(2)
							.contains(InMatcher.isIn(Set.of(123)), comparing234);
				});
	}
}
