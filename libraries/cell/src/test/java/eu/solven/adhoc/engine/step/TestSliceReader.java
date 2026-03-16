/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.engine.step;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.IValueMatcher;

public class TestSliceReader {
	@Test
	public void testRead() {
		SliceReader reader = SliceReader.builder()
				.sliceFilter(AndFilter.and(Map.of("a", "a1")))
				.stepFilter(AndFilter.and(Map.of("b", "b2")))
				.build();

		Assertions.assertThat(reader.getValueMatcher("a")).isEqualTo(EqualsMatcher.matchEq("a1"));
		Assertions.assertThat(reader.getValueMatcher("b")).isEqualTo(EqualsMatcher.matchEq("b2"));
	}

	@Test
	public void testIncompatible() {
		SliceReader reader = SliceReader.builder()
				.sliceFilter(AndFilter.and(Map.of("a", "a1")))
				.stepFilter(AndFilter.and(Map.of("a", "a2")))
				.build();

		Assertions.assertThat(reader.getValueMatcher("unknown")).isEqualTo(IValueMatcher.MATCH_ALL);
		Assertions.assertThat(reader.getValueMatcher("a")).isEqualTo(EqualsMatcher.matchEq("a1"));

		Assertions.assertThatThrownBy(() -> reader.asFilter()).isInstanceOf(IllegalStateException.class);
	}
}
