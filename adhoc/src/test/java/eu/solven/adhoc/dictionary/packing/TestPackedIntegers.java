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

import java.util.Random;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.compression.packing.PackedIntegers;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestPackedIntegers {

	private PackedIntegers doPack(int[] input) {
		return PackedIntegers.doPack(input);
	}

	private void doCheck(int[] input, PackedIntegers packed) {
		for (int i = 0; i < input.length; i++) {
			Assertions.assertThat(packed.readInt(i)).isEqualTo(input[i]);
		}
	}

	@Test
	public void testPacking_1Entry0() {
		int[] input = new int[] { 0 };
		PackedIntegers packed = doPack(input);

		doCheck(input, packed);
	}

	@Test
	public void testPacking() {
		int[] input = new int[] { 0, 1, 2 };
		PackedIntegers packed = doPack(input);

		doCheck(input, packed);
	}

	@Test
	public void testPacking_0to64() {
		int[] input = IntStream.range(0, 64).toArray();
		PackedIntegers packed = doPack(input);

		doCheck(input, packed);
	}

	@Test
	public void testPacking_0ToGrowingMax() {
		for (int max = 1; max < 128; max++) {
			int[] input = IntStream.range(0, max).toArray();
			PackedIntegers packed = doPack(input);

			doCheck(input, packed);
		}
	}

	@Test
	public void testPacking_GrowingMinToGrowingMax() {
		for (int min = 1; min < 64; min++) {
			for (int max = 0; max < 64; max++) {
				int[] input = IntStream.range(min, min + max).toArray();
				PackedIntegers packed = doPack(input);

				doCheck(input, packed);
			}
		}
	}

	@Test
	public void testEmpty() {
		int[] input = new int[] {};
		PackedIntegers packed = doPack(input);

		Assertions.assertThatThrownBy(() -> packed.readInt(0)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
	}

	@Test
	public void testIntegerMax() {
		int[] input = new int[] { 0, 1, Integer.MAX_VALUE };
		PackedIntegers packed = doPack(input);

		doCheck(input, packed);
	}

	@Test
	public void testNegative() {
		int[] input = new int[] { 0, 1, Integer.MAX_VALUE, -1 };
		PackedIntegers packed = doPack(input);

		doCheck(input, packed);
	}

	@Test
	public void testFuzzy() {
		Random r = new Random();
		long seed = r.nextLong();
		log.info("seed={}", seed);
		Random r2 = new Random(seed);

		for (int iterationIndex = 0; iterationIndex < 1024; iterationIndex++) {
			int[] input = IntStream.range(0, r2.nextInt(128)).map(i -> r2.nextInt()).toArray();
			PackedIntegers packed = doPack(input);

			doCheck(input, packed);
		}
	}
}
