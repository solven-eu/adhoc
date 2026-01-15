package eu.solven.adhoc.dictionary.packing;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;
import me.lemire.integercompression.BitPacking;
import me.lemire.integercompression.Util;

@Slf4j
public class TestPackedIntegers {

	private PackedIntegers doPack(int[] input) {
		// fastpackwithoutmask requires input to have size % 32
		int[] input32;

		if (input.length % 32 != 0) {
			input32 = Arrays.copyOf(input, 32 * (1 + input.length / 32));
		} else {
			input32 = input;
		}

		// `fastpackwithoutmask` will pack 32 integers into this number of integers
		final int bits = Util.maxbits(input32, 0, input.length);

		int[] output = new int[bits];

		BitPacking.fastpackwithoutmask(input32, 0, output, 0, bits);

		PackedIntegers packed = PackedIntegers.builder().bits(bits).length(input.length).holder(output).build();
		return packed;
	}

	private void doCheck(int[] input, PackedIntegers packed) {
		for (int i = 0; i < input.length; i++) {
			Assertions.assertThat(packed.readInt(i)).isEqualTo(input[i]);
		}
	}

	@Test
	public void testPacking() {
		int[] input = new int[] { 0, 1, 2 };
		PackedIntegers packed = doPack(input);

		doCheck(input, packed);
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
