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
package eu.solven.adhoc.cuboid;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.primitive.IValueProvider;

public class TestSliceAndMeasure {

	@Test
	public void testToString_long() {
		SliceAndMeasure<String> sm =
				SliceAndMeasure.<String>builder().slice("mySlice").valueProvider(vc -> vc.onLong(42L)).build();

		Assertions.assertThat(sm.toString()).isEqualTo("mySlice=42");
	}

	@Test
	public void testToString_double() {
		SliceAndMeasure<String> sm =
				SliceAndMeasure.<String>builder().slice("s").valueProvider(vc -> vc.onDouble(3.14)).build();

		Assertions.assertThat(sm.toString()).isEqualTo("s=3.14");
	}

	@Test
	public void testToString_object() {
		SliceAndMeasure<String> sm =
				SliceAndMeasure.<String>builder().slice("s").valueProvider(vc -> vc.onObject("hello")).build();

		Assertions.assertThat(sm.toString()).isEqualTo("s=hello");
	}

	@Test
	public void testToString_null() {
		SliceAndMeasure<String> sm =
				SliceAndMeasure.<String>builder().slice("s").valueProvider(IValueProvider.NULL).build();

		Assertions.assertThat(sm.toString()).isEqualTo("s=null");
	}

	@Test
	public void testToString_integerSlice() {
		SliceAndMeasure<Integer> sm =
				SliceAndMeasure.<Integer>builder().slice(7).valueProvider(vc -> vc.onLong(99L)).build();

		Assertions.assertThat(sm.toString()).isEqualTo("7=99");
	}
}
