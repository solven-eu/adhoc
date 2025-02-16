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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import eu.solven.adhoc.util.NotYetImplementedException;

public class TestInMatcher {
	@Test
	public void testNested() {
		Assertions.assertThat(InMatcher.isIn("a", "b"))
				.isInstanceOfSatisfying(InMatcher.class,
						in -> Assertions.assertThat(in.getOperands()).isEqualTo(Set.of("a", "b")));
		Assertions.assertThat(InMatcher.isIn(Set.of("a", "b")))
				.isInstanceOfSatisfying(InMatcher.class,
						in -> Assertions.assertThat(in.getOperands()).isEqualTo(Set.of("a", "b")));
		Assertions.assertThat(InMatcher.isIn(List.of("a", "b")))
				.isInstanceOfSatisfying(InMatcher.class,
						in -> Assertions.assertThat(in.getOperands()).isEqualTo(Set.of("a", "b")));

		Assertions.assertThat(InMatcher.isIn(List.of("a", "b"), "c"))
				.isInstanceOfSatisfying(InMatcher.class,
						in -> Assertions.assertThat(in.getOperands()).isEqualTo(Set.of("a", "b", "c")));
	}

	@Test
	public void testSingleNull() {
		Set<Object> singletonNull = new HashSet<>();
		singletonNull.add(null);

		Assertions.assertThat(InMatcher.isIn(singletonNull)).isInstanceOf(NullMatcher.class);
	}

	@Test
	public void testNullAndNotNull() {
		Set<Object> singletonNull = new HashSet<>();
		singletonNull.add(null);
		singletonNull.add("notNull");

		// Assertions.assertThat(InMatcher.isIn(singletonNull)).isInstanceOf(InMatcher.class);
		Assertions.assertThatThrownBy(() -> InMatcher.isIn(singletonNull))
				.isInstanceOf(NotYetImplementedException.class);
	}
}
