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

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import eu.solven.adhoc.query.filter.value.EqualsDoubleMatcher;
import eu.solven.adhoc.query.filter.value.EqualsLongMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.EqualsObjectMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import eu.solven.adhoc.query.filter.value.StringMatcher;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TestEqualsMatcher {
	@Test
	public void testEqualsHashcode() {
		EqualsVerifier.forClass(EqualsLongMatcher.class).verify();
		EqualsVerifier.forClass(EqualsDoubleMatcher.class).verify();
		EqualsVerifier.forClass(EqualsObjectMatcher.class).verify();
	}

	@Test
	public void testSimple() {
		IValueMatcher equalsMatcher = EqualsMatcher.matchEq("a");

		Assertions.assertThat(equalsMatcher.match("a")).isEqualTo(true);
		Assertions.assertThat(equalsMatcher.match(Set.of("a"))).isEqualTo(false);

		Assertions.assertThat(equalsMatcher.match(null)).isEqualTo(false);
	}

	@Test
	public void testNull() {
		Assertions.assertThatThrownBy(() -> EqualsObjectMatcher.builder().operand(null).build())
				.isInstanceOf(IllegalArgumentException.class);

		Assertions.assertThat(EqualsMatcher.matchEq(null)).isInstanceOf(NullMatcher.class);
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		IValueMatcher matcher = EqualsMatcher.matchEq("azerty");

		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		String asString = objectMapper.writeValueAsString(matcher);
		Assertions.assertThat(asString).isEqualToNormalizingNewlines("""
								"azerty"
				""".trim());

		IValueMatcher fromString = objectMapper.readValue(asString, IValueMatcher.class);

		Assertions.assertThat(fromString).isEqualTo(matcher);
	}

	@Disabled("https://github.com/FasterXML/jackson-databind/issues/5030")
	@Test
	public void testJackson_toMap() throws JsonProcessingException {
		IValueMatcher matcher = EqualsMatcher.matchEq("azerty");

		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		Map asMap = objectMapper.convertValue(matcher, Map.class);
		Assertions.assertThat(asMap).hasSize(2).containsEntry("type", "equals");

		IValueMatcher fromString = objectMapper.convertValue(asMap, IValueMatcher.class);

		Assertions.assertThat(fromString).isEqualTo(matcher);
	}

	// Ensure we can parse a EqualsMatcher with the default serialization format
	@Test
	public void testJackson_fromExplicit() throws JsonProcessingException {
		IValueMatcher matcher = EqualsMatcher.matchEq("azerty");

		ObjectMapper objectMapper = new ObjectMapper();

		String asString = """
				{
				    "type" : "equals",
				    "operand" : "azerty"
				  }
				""";

		IValueMatcher fromString = objectMapper.readValue(asString, IValueMatcher.class);

		Assertions.assertThat(fromString).isEqualTo(matcher);
	}

	@Test
	public void testJackson_fromExplicit_int() throws JsonProcessingException {
		IValueMatcher matcher = EqualsMatcher.matchEq(3);

		ObjectMapper objectMapper = new ObjectMapper();

		String asString = """
				{
				    "type" : "equals",
				    "operand" : 3
				  }
				""";

		IValueMatcher fromString = objectMapper.readValue(asString, IValueMatcher.class);

		Assertions.assertThat(fromString).isEqualTo(matcher);
	}

	@Test
	public void testJackson_integer() throws JsonProcessingException {
		IValueMatcher matcher = EqualsMatcher.matchEq(3);

		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		String asString = objectMapper.writeValueAsString(matcher);
		Assertions.assertThat(asString).isEqualTo("3");

		IValueMatcher fromString = objectMapper.readValue(asString, IValueMatcher.class);

		Assertions.assertThat(fromString).isEqualTo(matcher);
	}

	// When receiving a filter through json, we have no guarantee regarding the actual type
	// Still, one generally expect when filtering that `3 == 3L`
	@Test
	public void testIntEqualsLong() throws JsonProcessingException {
		IValueMatcher matcher = EqualsMatcher.matchEq(3);

		Assertions.assertThat(matcher.match((int) 3)).isTrue();
		Assertions.assertThat(matcher.match((int) 4)).isFalse();

		Assertions.assertThat(matcher.match((long) 3)).isTrue();
		Assertions.assertThat(matcher.match((long) 4)).isFalse();

		Assertions.assertThat(new ObjectMapper().writeValueAsString((3.0))).isEqualTo("3.0");

		// It is unclear if we should also match float and doubles
		// For now, we do match as
		Assertions.assertThat(matcher.match((float) 3)).isTrue();
		Assertions.assertThat(matcher.match((double) 3)).isTrue();
	}

	// This checks commutativity of equals on cross-type edge-case
	@Test
	public void testIntEqualsFloat() throws JsonProcessingException {
		Assertions.assertThat(EqualsMatcher.matchEq(3).match((float) 3)).isTrue();
		Assertions.assertThat(EqualsMatcher.matchEq(3.0).match(3)).isTrue();
	}

	@Test
	public void testIntEqualsString() throws JsonProcessingException {
		Assertions.assertThat(EqualsMatcher.matchEq(3).match("3")).isFalse();
		Assertions.assertThat(EqualsMatcher.matchEq("3").match(3)).isFalse();
	}

	// When receiving a filter through json, we have no guarantee regarding the actual type
	// Still, one generally expect when filtering that `1.2 == 1.2D`
	@Test
	public void testFloatEqualsDouble_imperfect() throws JsonProcessingException {
		IValueMatcher matcher = EqualsMatcher.matchEq(1.2);

		// Not equals to `1.2F` as `1.2` is not perfectible representable by a float
		Assertions.assertThat(matcher.match((float) 1.2)).isFalse();
		Assertions.assertThat(matcher.match((double) 1.2)).isTrue();

		// It is unclear if we should also match float and doubles
		Assertions.assertThat(matcher.match((int) 3)).isFalse();
		Assertions.assertThat(matcher.match((long) 3)).isFalse();
	}

	@Test
	public void testFloatEqualsDouble_perfect() throws JsonProcessingException {
		IValueMatcher matcher = EqualsMatcher.matchEq(0.5);

		// Equals to `0.5F` as `0.5` is not perfectible representable by a float
		Assertions.assertThat(matcher.match((float) 0.5)).isTrue();
		Assertions.assertThat(matcher.match((double) 0.5)).isTrue();

		// It is unclear if we should also match float and doubles
		Assertions.assertThat(matcher.match((int) 3)).isFalse();
		Assertions.assertThat(matcher.match((long) 3)).isFalse();
	}

	@Test
	public void testFloatEqualsDouble_similarToInt() throws JsonProcessingException {
		IValueMatcher matcher = EqualsMatcher.matchEq(3D);

		Assertions.assertThat(matcher.match((float) 3)).isTrue();
		Assertions.assertThat(matcher.match((double) 3)).isTrue();

		Assertions.assertThat(matcher.match((int) 3)).isTrue();
		Assertions.assertThat(matcher.match((long) 3)).isTrue();
	}

	@Test
	public void testExtractOperand() {
		// Not EqualsMatcher
		Assertions.assertThat(EqualsMatcher.extractOperand(InMatcher.matchIn("a", "b"))).isEmpty();

		// Simple matching case
		Assertions.assertThat(EqualsMatcher.extractOperand(EqualsMatcher.matchEq("a"), String.class)).contains("a");
		Assertions.assertThat((Optional) EqualsMatcher.extractOperand(EqualsMatcher.matchEq("a"))).contains("a");

		// sub type
		Assertions.assertThat(EqualsMatcher.extractOperand(EqualsMatcher.matchEq("a"), CharSequence.class))
				.contains("a");

		// incompatible type
		Assertions.assertThat(EqualsMatcher.extractOperand(EqualsMatcher.matchEq("a"), Double.class)).isEmpty();
	}

	@Test
	public void testValueMatcherOperand() {
		IValueMatcher operandValueMatcher = StringMatcher.hasToString("foo");
		IValueMatcher equalsMatcher = EqualsMatcher.matchEq(operandValueMatcher);

		Assertions.assertThat(equalsMatcher).isEqualTo(operandValueMatcher);
	}

	@Test
	public void testToString() {
		Assertions.assertThat(EqualsMatcher.matchEq(123).toString()).isEqualTo("==123");
		Assertions.assertThat(EqualsMatcher.matchEq(12.34).toString()).isEqualTo("==12.34");
		Assertions.assertThat(EqualsMatcher.matchEq("foo").toString()).isEqualTo("==foo");
	}
}
