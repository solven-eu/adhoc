package eu.solven.adhoc.filter.value;

import java.time.LocalDate;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.StringMatcher;

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
	public void testJackson() throws JsonProcessingException {
		IValueMatcher stringMatcher = StringMatcher.hasToString(today);

		ObjectMapper objectMapper = new ObjectMapper();
		// https://stackoverflow.com/questions/17617370/pretty-printing-json-from-jackson-2-2s-objectmapper
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

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
