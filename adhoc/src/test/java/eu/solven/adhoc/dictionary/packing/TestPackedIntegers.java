package eu.solven.adhoc.dictionary.packing;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.compression.packing.PackedIntegers;
import lombok.extern.slf4j.Slf4j;
import me.lemire.integercompression.BitPacking;
import me.lemire.integercompression.Util;

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
		for (int max = 0; max < 128; max++) {
			int[] input = IntStream.range(0, max).toArray();
			PackedIntegers packed = doPack(input);

			doCheck(input, packed);
		}
	}

	@Test
	public void testPacking_GrowingMinToGrowingMax() {
		for (int min = 0; min < 64; min++) {
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
