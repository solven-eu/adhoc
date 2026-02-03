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
package eu.solven.adhoc.dictionary.packing;

import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.compression.IIntArray;
import eu.solven.adhoc.compression.packing.FlexiblePackedIntegers;
import eu.solven.adhoc.compression.packing.SingleChunkPackedIntegers;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestSingleChunkPackedIntegers {

	private IIntArray doPack(int[] input) {
		return FlexiblePackedIntegers.doPack(input);
	}

	private void doCheck(int[] input, IIntArray packed) {
		Assertions.assertThat(packed).as("length=%s", input.length).isInstanceOf(SingleChunkPackedIntegers.class);

		for (int i = 0; i < input.length; i++) {
			Assertions.assertThat(packed.readInt(i)).isEqualTo(input[i]);
		}
	}

	@Test
	public void testPacking_1Entry253() {
		int[] input = new int[] { 253 };
		IIntArray packed = doPack(input);

		doCheck(input, packed);
	}

	@Test
	public void testPacking_0to1() {
		int[] input = new int[] { 0, 1 };
		IIntArray packed = doPack(input);

		doCheck(input, packed);
	}

	@Test
	public void testPacking_0to4() {
		Assertions.assertThat(4).isEqualTo(1 << 2);

		int[] input = IntStream.range(0, 1 << 2).toArray();
		IIntArray packed = doPack(input);

		doCheck(input, packed);
	}

	@Test
	public void testPacking_0to256() {
		Assertions.assertThat(256).isEqualTo(1 << 8);

		int[] input = IntStream.range(0, 1 << 8).toArray();
		IIntArray packed = doPack(input);

		doCheck(input, packed);
	}

	@Test
	public void testPacking_0ToGrowingMax() {
		for (int maxPower2 = 0; maxPower2 < 5; maxPower2++) {
			// 1, 2, 8, 128, 32768
			// `-2147483648` is not tested here as `.rangeClosed` would not handle it
			int max = (1 << ((1 << maxPower2) - 1));
			int[] input = IntStream.rangeClosed(1, max).toArray();
			IIntArray packed = doPack(input);

			doCheck(input, packed);
		}
	}

	@Test
	public void testNegative() {
		int[] input = new int[] { 0, 1, -1 };
		IIntArray packed = doPack(input);

		doCheck(input, packed);
	}

	@Test
	public void testEmpty() {
		int[] input = new int[] {};
		IIntArray packed = doPack(input);

		Assertions.assertThatThrownBy(() -> packed.readInt(0)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
	}

	@Test
	public void testMinus1() {
		int[] input = new int[] { 0, 1, -1 };
		IIntArray packed = doPack(input);

		doCheck(input, packed);
	}
}
