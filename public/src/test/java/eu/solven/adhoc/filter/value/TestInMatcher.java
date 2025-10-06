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

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import eu.solven.adhoc.query.filter.value.StringMatcher;

public class TestInMatcher {
	@Test
	public void testNested() {
		Assertions.assertThat(InMatcher.matchIn("a", "b"))
				.isInstanceOfSatisfying(InMatcher.class,
						in -> Assertions.assertThat(in.getOperands()).isEqualTo(Set.of("a", "b")));
		Assertions.assertThat(InMatcher.matchIn(Set.of("a", "b")))
				.isInstanceOfSatisfying(InMatcher.class,
						in -> Assertions.assertThat(in.getOperands()).isEqualTo(Set.of("a", "b")));
		Assertions.assertThat(InMatcher.matchIn(List.of("a", "b")))
				.isInstanceOfSatisfying(InMatcher.class,
						in -> Assertions.assertThat(in.getOperands()).isEqualTo(Set.of("a", "b")));

		Assertions.assertThat(InMatcher.matchIn(List.of("a", "b"), "c"))
				.isInstanceOfSatisfying(InMatcher.class,
						in -> Assertions.assertThat(in.getOperands()).isEqualTo(Set.of("a", "b", "c")));
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		IValueMatcher matcher = InMatcher.matchIn("a", "b");

		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		String asString = objectMapper.writeValueAsString(matcher);
		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
				{
				  "type" : "in",
				  "operands" : [ "a", "b" ]
				}
								""".trim());

		IValueMatcher fromString = objectMapper.readValue(asString, IValueMatcher.class);

		Assertions.assertThat(fromString).isEqualTo(matcher);
	}

	@Test
	public void testSingleNull() {
		Set<Object> singletonNull = new HashSet<>();
		singletonNull.add(null);

		Assertions.assertThat(InMatcher.matchIn(singletonNull)).isInstanceOf(NullMatcher.class);
	}

	@Test
	public void testNullAndNotNull() {
		Set<Object> singletonNull = new HashSet<>();
		singletonNull.add(null);
		singletonNull.add("notNull");

		Assertions.assertThat(InMatcher.matchIn(singletonNull))
				.isEqualTo(OrMatcher.or(NullMatcher.matchNull(), EqualsMatcher.matchEq("notNull")));
	}

	@Test
	public void testNullInNestedCollection() {
		Set<Object> nestedMayNull = new HashSet<>();
		nestedMayNull.add(Arrays.asList("foo"));
		nestedMayNull.add(Arrays.asList(null, "bar"));

		Assertions.assertThat(InMatcher.matchIn(nestedMayNull))
				.isEqualTo(OrMatcher.or(NullMatcher.matchNull(), InMatcher.matchIn("foo", "bar")));
	}

	@Test
	public void testNullAndNotNull1And2() {
		Set<Object> singletonNull = new HashSet<>();
		singletonNull.add(null);
		singletonNull.add("notNull1");
		singletonNull.add("notNull2");

		Assertions.assertThat(InMatcher.matchIn(singletonNull))
				.isEqualTo(OrMatcher.or(NullMatcher.matchNull(), InMatcher.matchIn("notNull1", "notNull2")));
	}

	@Test
	public void testGetOperands() {
		// equals case
		Assertions.assertThat(InMatcher.extractOperands(EqualsMatcher.matchEq("foo"), String.class))
				.containsExactly("foo");

		// Incompatible class
		Assertions.assertThat(InMatcher.extractOperands(EqualsMatcher.matchEq("foo"), LocalDate.class)).isEmpty();

		Assertions.assertThat(InMatcher.extractOperands(InMatcher.matchIn("foo", LocalDate.now(), "bar"), String.class))
				.containsExactly("foo", "bar");

		Assertions.assertThat(InMatcher.extractOperands(NotMatcher.not(EqualsMatcher.matchEq("foo")), String.class))
				.isEmpty();
	}

	@Test
	public void testValueMatcherOperand() {
		IValueMatcher operandValueMatcher = StringMatcher.hasToString("foo");

		IValueMatcher inMatcher = InMatcher.matchIn(operandValueMatcher, "bar");

		Assertions.assertThat(inMatcher)
				.isEqualTo(
						OrMatcher.builder().operand(operandValueMatcher).operand(EqualsMatcher.matchEq("bar")).build());
	}

	@Test
	public void testMultipleInts() {
		IValueMatcher or123_234 = OrMatcher.or(EqualsMatcher.matchEq(123), EqualsMatcher.matchEq(234));
		Assertions.assertThat(or123_234.match(123)).isTrue();
	}

	@Test
	public void testInIntMatchLong() {
		Assertions.assertThat(InMatcher.matchIn(123, 234).match(123L)).isTrue();
		Assertions.assertThat(InMatcher.matchIn(123L, 234L).match(123)).isTrue();

		Assertions.assertThat(InMatcher.matchIn(12.34, 23.45).match(12.34D)).isTrue();
		Assertions.assertThat(InMatcher.matchIn(12.34D, 23.45D).match(12.34)).isTrue();
	}

	@Test
	public void testInDoubleMatchLong() {
		Assertions.assertThat(InMatcher.matchIn(123D, 234D).match(123L)).isFalse();
		Assertions.assertThat(InMatcher.matchIn(123L, 234L).match(123D)).isFalse();
	}

	@Test
	public void testIn_positive_hardcoded() {
		Assertions.assertThat(InMatcher.matchIn(ComparingMatcher.builder().greaterThan(123).build(), 234D))
				.isInstanceOf(ComparingMatcher.class);
	}

	@Test
	public void testToString_huge() {
		IValueMatcher inMatcher = InMatcher.matchIn(IntStream.range(0, 1024).mapToObj(i -> i).toList());

		Assertions.assertThat(inMatcher)
				.hasToString(
						"InMatcher{size=1024, #0=0, #1=1, #2=2, #3=3, #4=4, #5=5, #6=6, #7=7, #8=8, #9=9, #10=10, #11=11, #12=12, #13=13, #14=14, #15=15, #16=1008 more entries}")
				.isInstanceOfSatisfying(InMatcher.class, m -> {
					Assertions.assertThat(m.toString("c", false))
							.isEqualTo("c=in=(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15, and 1008 more entries)");
				});
	}
}
