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

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.sum.CoalesceAggregation;

public class TestCoalesceAggregation {

	@Test
	public void testSimple() {
		CoalesceAggregation a = new CoalesceAggregation(Map.of());

		Assertions.assertThat(a.aggregate((Object) null, null)).isEqualTo(null);
		Assertions.assertThat(a.aggregate(null, 1.2D)).isEqualTo(1.2D);
		Assertions.assertThat(a.aggregate(1.2D, null)).isEqualTo(1.2D);

		Assertions.assertThat(a.aggregate(123, 123)).isEqualTo(123);
		Assertions.assertThat(a.aggregate("foo", "foo")).isEqualTo("foo");

		Assertions.assertThat(a.aggregate(123, 234)).isEqualTo(123);
		Assertions.assertThat(a.aggregate("foo", 234)).isEqualTo("foo");
	}

	@Test
	public void testFailIfDifferent() {
		CoalesceAggregation a = new CoalesceAggregation(Map.of("failIfDifferent", true));

		Assertions.assertThat(a.aggregate((Object) null, null)).isEqualTo(null);
		Assertions.assertThat(a.aggregate(null, 1.2D)).isEqualTo(1.2D);
		Assertions.assertThat(a.aggregate(1.2D, null)).isEqualTo(1.2D);

		Assertions.assertThat(a.aggregate(123, 123)).isEqualTo(123);
		Assertions.assertThat(a.aggregate("foo", "foo")).isEqualTo("foo");

		Assertions.assertThatThrownBy(() -> a.aggregate(123, 234)).isInstanceOf(IllegalArgumentException.class);
		Assertions.assertThatThrownBy(() -> a.aggregate("foo", 234)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	public void testByRef() {
		CoalesceAggregation a = new CoalesceAggregation(Map.of("matchReferences", true));

		Assertions.assertThat(a.aggregate((Object) null, null)).isEqualTo(null);
		Assertions.assertThat(a.aggregate(null, 1.2D)).isEqualTo(1.2D);
		Assertions.assertThat(a.aggregate(1.2D, null)).isEqualTo(1.2D);

		// Small integers objects are cached
		Assertions.assertThat(a.aggregate(123, 123)).isEqualTo(123);
		// But not large ones
		Assertions.assertThat(a.aggregate(Integer.MAX_VALUE, Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);

		Assertions.assertThat(a.aggregate("foo", "foo")).isEqualTo("foo");

		Assertions.assertThat(a.aggregate(123, 234)).isEqualTo(123);
		Assertions.assertThat(a.aggregate("foo", 234)).isEqualTo("foo");
	}

	@Test
	public void testByRef_failIfDifferent() {
		CoalesceAggregation a = new CoalesceAggregation(Map.of("matchReferences", true, "failIfDifferent", true));

		Assertions.assertThat(a.aggregate((Object) null, null)).isEqualTo(null);
		Assertions.assertThat(a.aggregate(null, 1.2D)).isEqualTo(1.2D);
		Assertions.assertThat(a.aggregate(1.2D, null)).isEqualTo(1.2D);

		// Small integers objects are cached
		Assertions.assertThat(a.aggregate(123, 123)).isEqualTo(123);
		// But not large ones
		Assertions.assertThatThrownBy(() -> a.aggregate(Integer.MAX_VALUE, Integer.MAX_VALUE))
				.isInstanceOf(IllegalArgumentException.class)
				.hasStackTraceContaining("java.lang.Integer@");

		// Did the compiler ensured these 2 strings have same ref?
		Assertions.assertThat(a.aggregate("foo", "foo")).isEqualTo("foo");

		Assertions.assertThatThrownBy(() -> a.aggregate(123, 234))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("123(java.lang.Integer) != 234(java.lang.Integer)");
		Assertions.assertThatThrownBy(() -> a.aggregate("foo", 234)).isInstanceOf(IllegalArgumentException.class);
	}
}
