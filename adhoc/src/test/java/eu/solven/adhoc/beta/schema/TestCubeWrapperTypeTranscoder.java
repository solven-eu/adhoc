package eu.solven.adhoc.beta.schema;

import java.time.LocalDate;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.StringMatcher;

public class TestCubeWrapperTypeTranscoder {

	@Test
	public void testParseString() {
		CubeWrapperTypeTranscoder typeTranscoder =
				CubeWrapperTypeTranscoder.builder().cubeColumn("c", String.class).build();

		Assertions.assertThat(typeTranscoder.mayTranscode("c")).isTrue();
		Assertions.assertThat(typeTranscoder.toTable("c", "someString")).isEqualTo("someString");
		Assertions.assertThat(typeTranscoder.toTable("c", EqualsMatcher.isEqualTo("someString")))
				.isEqualTo(EqualsMatcher.isEqualTo("someString"));
	}

	@Test
	public void testParseLocalDate() {
		CubeWrapperTypeTranscoder typeTranscoder =
				CubeWrapperTypeTranscoder.builder().cubeColumn("c", LocalDate.class).build();

		Assertions.assertThat(typeTranscoder.mayTranscode("c")).isTrue();
		Assertions.assertThat(typeTranscoder.toTable("c", "2025-06-04")).isEqualTo(LocalDate.parse("2025-06-04"));
		Assertions.assertThat(typeTranscoder.toTable("c", EqualsMatcher.isEqualTo("2025-06-04")))
				.isEqualTo(EqualsMatcher.isEqualTo(LocalDate.parse("2025-06-04")));

		Assertions.assertThat(typeTranscoder.toTable("c", InMatcher.isIn("2025-06-04", "2025-06-05")))
				.isEqualTo(InMatcher.isIn(LocalDate.parse("2025-06-04"), LocalDate.parse("2025-06-05")));
	}

	private static class CustomClass {

	}

	@Test
	public void testParseCustomClass() {
		CubeWrapperTypeTranscoder typeTranscoder =
				CubeWrapperTypeTranscoder.builder().cubeColumn("c", CustomClass.class).build();

		Assertions.assertThat(typeTranscoder.mayTranscode("c")).isTrue();
		Assertions.assertThat(typeTranscoder.toTable("c", "someStringForCustom"))
				.isEqualTo(StringMatcher.hasToString("someStringForCustom"));
	}
}
