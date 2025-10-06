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
				CubeWrapperTypeTranscoder.builder().columnToType("c", String.class).build();

		Assertions.assertThat(typeTranscoder.mayTranscode("c")).isTrue();
		Assertions.assertThat(typeTranscoder.toTable("c", "someString")).isEqualTo("someString");
		Assertions.assertThat(typeTranscoder.toTable("c", EqualsMatcher.matchEq("someString")))
				.isEqualTo(EqualsMatcher.matchEq("someString"));
	}

	@Test
	public void testParseLocalDate() {
		CubeWrapperTypeTranscoder typeTranscoder =
				CubeWrapperTypeTranscoder.builder().columnToType("c", LocalDate.class).build();

		Assertions.assertThat(typeTranscoder.mayTranscode("c")).isTrue();
		Assertions.assertThat(typeTranscoder.toTable("c", "2025-06-04")).isEqualTo(LocalDate.parse("2025-06-04"));
		Assertions.assertThat(typeTranscoder.toTable("c", EqualsMatcher.matchEq("2025-06-04")))
				.isEqualTo(EqualsMatcher.matchEq(LocalDate.parse("2025-06-04")));

		Assertions.assertThat(typeTranscoder.toTable("c", InMatcher.matchIn("2025-06-04", "2025-06-05")))
				.isEqualTo(InMatcher.matchIn(LocalDate.parse("2025-06-04"), LocalDate.parse("2025-06-05")));
	}

	private static class CustomClass {

	}

	@Test
	public void testParseCustomClass() {
		CubeWrapperTypeTranscoder typeTranscoder =
				CubeWrapperTypeTranscoder.builder().columnToType("c", CustomClass.class).build();

		Assertions.assertThat(typeTranscoder.mayTranscode("c")).isTrue();
		Assertions.assertThat(typeTranscoder.toTable("c", "someStringForCustom"))
				.isEqualTo(StringMatcher.hasToString("someStringForCustom"));
	}
}
