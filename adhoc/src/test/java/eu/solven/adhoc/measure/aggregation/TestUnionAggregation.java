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
package eu.solven.adhoc.measure.aggregation;

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.aggregation.collection.UnionAggregation;

public class TestUnionAggregation {
	IAggregation union = new UnionAggregation();

	@Test
	public void testNull_null() {
		Assertions.assertThat(union.aggregate((Object) null, null)).isEqualTo(null);
	}

	@Test
	public void testNull_set() {
		Assertions.assertThat(union.aggregate(null, 123)).isEqualTo(Set.of(123));
		Assertions.assertThat(union.aggregate(123, null)).isEqualTo(Set.of(123));

		Assertions.assertThat(union.aggregate(123, "foo")).isEqualTo(Set.of(123, "foo"));

		Assertions.assertThat(union.aggregate(null, Set.of(123))).isEqualTo(Set.of(123));
		Assertions.assertThat(union.aggregate(Set.of(123), null)).isEqualTo(Set.of(123));

		Assertions.assertThat(union.aggregate(123, Set.of("foo"))).isEqualTo(Set.of(123, "foo"));
	}

	@Test
	public void testNull_map() {
		Assertions.assertThat(union.aggregate(null, Map.of("k", "v"))).isEqualTo(Map.of("k", "v"));
		Assertions.assertThat(union.aggregate(Map.of("k", "v"), null)).isEqualTo(Map.of("k", "v"));

		Assertions.assertThat(union.aggregate(Map.of("k", "v"), Map.of("k2", "v2")))
				.isEqualTo(Map.of("k", "v", "k2", "v2"));
	}

	@Test
	public void testNull_setAndMap() {
		Assertions.assertThat(union.aggregate(Set.of("s"), Map.of("k", "v"))).isEqualTo(Set.of("s", Map.of("k", "v")));
	}
}
