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
package eu.solven.adhoc.measure.aggregation.collection;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestUnionListAggregator {
	@Test
	public void testUnion() {
		UnionListAggregation aggregation = new UnionListAggregation();

		Assertions.assertThat(aggregation.aggregate((Object) null, null)).isNull();
		Assertions.assertThat(aggregation.aggregate(null, 123)).isEqualTo(List.of(123));

		Assertions.assertThat(aggregation.aggregate("foo", null)).isEqualTo(List.of("foo"));

		Assertions.assertThat(aggregation.aggregate(List.of("foo"), null)).isEqualTo(List.of("foo"));
		Assertions.assertThat(aggregation.aggregate(null, List.of("foo"))).isEqualTo(List.of("foo"));

		Assertions.assertThat(aggregation.aggregate(List.of("foo"), List.of("bar"))).isEqualTo(List.of("foo", "bar"));

		Assertions.assertThat(aggregation.aggregate(List.of("foo"), 123)).isEqualTo(List.of("foo", 123));

		// Check we do not re-use references
		{
			List<String> input = List.of("foo");
			List<?> output = aggregation.aggregate(input, null);
			Assertions.assertThat(output).isEqualTo(List.of("foo")).isNotSameAs(input);
		}
	}
}
