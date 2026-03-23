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
package eu.solven.adhoc.encoding.dictionary;

import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.encoding.IIntArray;
import eu.solven.adhoc.encoding.packing.PackedIntegers;

/**
 * Unit tests for the package-private {@link DictionarizedObjectColumn#checkPostCompression} method.
 */
public class TestDictionarizedObjectColumnCheckPostCompression {

	private MapDictionarizer buildDictionarizer(List<Object> intToObject, List<?> values) {
		MapDictionarizer d = MapDictionarizer.builder().intToObject(intToObject).build();
		values.forEach(d::toInt);
		return d;
	}

	@Test
	public void testCheckPostCompression_happyPath() {
		List<Object> intToObject = new ArrayList<>();
		List<String> values = List.of("a", "b", "a");
		MapDictionarizer d = buildDictionarizer(intToObject, values);

		int[] rowToDic = new int[values.size()];
		for (int i = 0; i < values.size(); i++) {
			rowToDic[i] = d.toInt(values.get(i));
		}

		IIntArray packed = PackedIntegers.doPack(rowToDic);

		// Must not throw
		Assertions
				.assertThatCode(() -> DictionarizedObjectColumn.checkPostCompression(values, d, values.size(), packed))
				.doesNotThrowAnyException();
	}

	@Test
	public void testCheckPostCompression_lengthMismatch_throws() {
		List<Object> intToObject = new ArrayList<>();
		List<String> values = List.of("a", "b");
		MapDictionarizer d = buildDictionarizer(intToObject, values);

		int[] rowToDic = new int[] { 0, 1 };
		IIntArray packed = PackedIntegers.doPack(rowToDic);

		// Size=3 while packed.length()=2 → should throw
		Assertions.assertThatThrownBy(() -> DictionarizedObjectColumn.checkPostCompression(values, d, 3, packed))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Invalid length");
	}

	@Test
	public void testCheckPostCompression_valueMismatch_throws() {
		List<Object> intToObject = new ArrayList<>();
		// Register "a" as index 0 and "b" as index 1
		MapDictionarizer d = buildDictionarizer(intToObject, List.of("a", "b"));

		// The packed array claims index 1 (="b") at position 0, but the original list has "a" at position 0
		int[] rowToDic = new int[] { 1, 0 };
		IIntArray packed = PackedIntegers.doPack(rowToDic);

		List<String> original = List.of("a", "b");

		Assertions
				.assertThatThrownBy(
						() -> DictionarizedObjectColumn.checkPostCompression(original, d, original.size(), packed))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Invalid value");
	}
}
