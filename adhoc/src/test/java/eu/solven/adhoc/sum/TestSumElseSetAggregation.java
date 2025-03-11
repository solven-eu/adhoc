/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.sum;

import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.measure.sum.SumElseSetAggregation;

public class TestSumElseSetAggregation {
	SumAggregation a = new SumElseSetAggregation();

	@Test
	public void testSum_objects() {
		Assertions.assertThat(a.aggregate((Object) null, null)).isEqualTo(null);
		Assertions.assertThat(a.aggregate(null, 1.2D)).isEqualTo(1.2D);
		Assertions.assertThat(a.aggregate(1.2D, null)).isEqualTo(1.2D);
	}

	@Test
	public void testSum_doubles() {
		Assertions.assertThat(a.aggregateDoubles(Double.NaN, 1.2D)).isNaN();
		Assertions.assertThat(a.aggregateDoubles(1.2D, Double.NaN)).isNaN();
		Assertions.assertThat(a.aggregateDoubles(Double.NaN, Double.NaN)).isNaN();
	}

	@Test
	public void testSum_String() {
		Assertions.assertThat(a.aggregate(null, "someString")).isEqualTo(Set.of("someString"));
		Assertions.assertThat(a.aggregate("otherString", "someString")).isEqualTo(Set.of("someString", "otherString"));
		Assertions.assertThat(a.aggregate(123, "someString")).isEqualTo(Set.of("someString"));

		Assertions.assertThat(a.aggregate(null, Set.of("someString"))).isEqualTo(Set.of("someString"));
		Assertions.assertThat(a.aggregate("otherString", Set.of("someString")))
				.isEqualTo(Set.of("someString", "otherString"));
		Assertions.assertThat(a.aggregate(123, Set.of("someString"))).isEqualTo(Set.of("someString"));

		Assertions.assertThat(a.aggregate(Set.of("a", "b"), Set.of("b", "c"))).isEqualTo(Set.of("a", "b", "c"));

		Assertions.assertThat(a.aggregate("otherString", List.of("someString")))
				.isEqualTo(Set.of("someString", "otherString"));
	}
}
