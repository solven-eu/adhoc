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
package eu.solven.adhoc.data.column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.primitive.IValueProvider;

public class TestMultitypeArray {
	MultitypeArray array = MultitypeArray.builder().build();

	@Test
	public void testCrossTypes_longThenDouble() {
		array.add().onLong(123L);
		array.add().onDouble(23.45);

		Assertions.assertThat(array.size()).isEqualTo(2);
		Assertions.assertThat(IValueProvider.getValue(array.read(0))).isEqualTo(123L);
		Assertions.assertThat(IValueProvider.getValue(array.read(1))).isEqualTo(23.45D);

		// TODO Should we rather migrate everything to double?
		Assertions.assertThat(array.valuesType).isEqualByComparingTo(IMultitypeConstants.MASK_OBJECT);
	}

	@Test
	public void testCrossTypes_doubleThenLong() {
		array.add().onDouble(23.45);
		array.add().onLong(123L);

		Assertions.assertThat(array.size()).isEqualTo(2);
		Assertions.assertThat(IValueProvider.getValue(array.read(0))).isEqualTo(23.45D);
		Assertions.assertThat(IValueProvider.getValue(array.read(1))).isEqualTo(123L);

		// TODO Should we rather migrate everything to double?
		Assertions.assertThat(array.valuesType).isEqualByComparingTo(IMultitypeConstants.MASK_OBJECT);
	}

	@Test
	public void testSet_fromNull() {
		array.add().onObject(null);
		Assertions.assertThat(array.valuesType).isEqualByComparingTo(IMultitypeConstants.MASK_OBJECT);

		array.set(0).onLong(123);
		Assertions.assertThat(array.valuesType).isEqualByComparingTo(IMultitypeConstants.MASK_OBJECT);
	}

	@Test
	public void testSet_long_fromLong() {
		array.add().onLong(123);
		Assertions.assertThat(array.valuesType).isEqualByComparingTo(IMultitypeConstants.MASK_LONG);

		array.set(0).onLong(123);
		Assertions.assertThat(array.valuesType).isEqualByComparingTo(IMultitypeConstants.MASK_LONG);
		array.set(0).onLong(234);
		Assertions.assertThat(array.valuesType).isEqualByComparingTo(IMultitypeConstants.MASK_LONG);

		array.add().onLong(345);
		Assertions.assertThat(array.valuesType).isEqualByComparingTo(IMultitypeConstants.MASK_LONG);

		array.add(1).onLong(456);
		Assertions.assertThat(array.valuesType).isEqualByComparingTo(IMultitypeConstants.MASK_LONG);

		Assertions.assertThat(IValueProviderTestHelpers.getLong(array.read(0))).isEqualTo(234L);
		Assertions.assertThat(IValueProviderTestHelpers.getLong(array.read(1))).isEqualTo(456L);
		Assertions.assertThat(IValueProviderTestHelpers.getLong(array.read(2))).isEqualTo(345L);

		array.add().onDouble(12.34D);
		Assertions.assertThat(array.valuesType).isEqualByComparingTo(IMultitypeConstants.MASK_OBJECT);
	}

	@Test
	public void testToString() {
		Assertions.assertThat(array.toString()).isEqualTo("empty");

		array.add().onObject(null);
		Assertions.assertThat(array.toString()).isEqualTo("type=object values=[null]");

		array.set(0).onLong(123);
		Assertions.assertThat(array.toString()).isEqualTo("type=object values=[123]");

		{
			array.clear();
			array.add().onLong(123);
			Assertions.assertThat(array.toString()).isEqualTo("type=long values=[%s]".formatted(123));
		}
		{
			array.clear();
			array.add().onDouble(12.34);
			Assertions.assertThat(array.toString()).isEqualTo("type=double values=[%s]".formatted(12.34));
		}
	}

	@Test
	public void testEmpty() {
		Assertions.assertThatThrownBy(() -> array.read(0)).isInstanceOf(IndexOutOfBoundsException.class);
	}

	@Test
	public void testBasicOperations_perType() {
		Map<Object, Consumer<MultitypeArray>> consumers = new HashMap<>();

		consumers.put(123L, a -> a.add().onLong(123));
		consumers.put(12.34D, a -> a.add().onDouble(12.34D));
		consumers.put("foo", a -> a.add().onObject("foo"));
		consumers.put(null, a -> a.add().onObject(null));

		consumers.forEach((value, consumer) -> {
			array.clear();

			consumer.accept(array);
			Assertions.assertThat(array.size()).isEqualTo(1);

			Object oInArray = array.apply(0, o -> o);
			Assertions.assertThat(oInArray).isEqualTo(value);

			array.remove(0);

			Assertions.assertThat(array.size()).isEqualTo(0);
		});
	}

	// The behavior of this test may change in later implementation
	// Typically, we may handle multiple types chunks, instead of switching all to objects
	@Test
	public void testReplaceAllObjects() {
		array.add().onLong(123);
		array.add().onObject(234);

		List<Object> replaced = new ArrayList<>();

		array.replaceAllObjects(o -> {
			replaced.add(o);

			return 345;
		});

		Assertions.assertThat(replaced).containsExactly(123L, 234);

		Assertions.assertThat(array.toString()).isEqualTo("type=object values=[345, 345]");
	}
}
