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
package eu.solven.adhoc.map.perfect_hashing;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.base.Strings;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestSimplePerfectHash {
	Random rr = new Random();
	long seed = rr.nextLong();

	@Test
	public void testPowerOfTwo() {
		// low values
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(0)).isEqualTo(1);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(1)).isEqualTo(1);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(2)).isEqualTo(2);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(3)).isEqualTo(4);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(4)).isEqualTo(4);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(5)).isEqualTo(8);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(6)).isEqualTo(8);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(7)).isEqualTo(8);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(8)).isEqualTo(8);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(9)).isEqualTo(16);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(10)).isEqualTo(16);

		// around Integer.MAX_VALUE / 2
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(Integer.MAX_VALUE / 2 - 1))
				.isEqualTo(Integer.MAX_VALUE / 2 + 1);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(Integer.MAX_VALUE / 2 - 1))
				.isEqualTo(Integer.MAX_VALUE / 2 + 1);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(Integer.MAX_VALUE / 2 + 1))
				.isEqualTo(Integer.MAX_VALUE / 2 + 1);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(Integer.MAX_VALUE / 2 + 2))
				.isEqualTo(Integer.MIN_VALUE);

		// around Integer.MAX_VALUE
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(Integer.MAX_VALUE - 1))
				.isEqualTo(Integer.MIN_VALUE);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(Integer.MAX_VALUE))
				.isEqualTo(Integer.MIN_VALUE);

		// These may be wrong behavior, but it follows some logic
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(-1)).isEqualTo(1);
		Assertions.assertThat(SimplePerfectHash.higherOrEqualsPowerOfTwo(-2)).isEqualTo(1);
	}

	@Test
	public void testHash_0() {
		IHasIndexOf<Object> hashed = SimplePerfectHash.make(List.of());

		Assertions.assertThat(hashed.indexOf("a")).isEqualTo(-1);
		Assertions.assertThat(hashed.indexOf("b")).isEqualTo(-1);

		Assertions.assertThat(hashed.unsafeIndexOf("a")).isEqualTo(0);
		Assertions.assertThat(hashed.unsafeIndexOf("b")).isEqualTo(0);

		Assertions.assertThat(hashed).isInstanceOfSatisfying(SimplePerfectHash.class, hh -> {
			Assertions.assertThat(hh.mask).isEqualTo(0);
			Assertions.assertThat(hh.remapped).hasSize(1);
		});
	}

	@Test
	public void testHash_1() {
		IHasIndexOf<String> hashed = SimplePerfectHash.make(List.of("a"));
		Assertions.assertThat(hashed.indexOf("a")).isEqualTo(0);
		Assertions.assertThat(hashed.indexOf("b")).isEqualTo(-1);

		Assertions.assertThat(hashed.unsafeIndexOf("a")).isEqualTo(0);
		Assertions.assertThat(hashed.unsafeIndexOf("b")).isEqualTo(0);

		Assertions.assertThat(hashed)
				.isInstanceOfSatisfying(SimplePerfectHash.class, hh -> Assertions.assertThat(hh.remapped).hasSize(1));
	}

	@Test
	public void testHash_2() {
		IHasIndexOf<String> hashed = SimplePerfectHash.make(List.of("a", "b"));
		Assertions.assertThat(hashed.indexOf("a")).isEqualTo(0);
		Assertions.assertThat(hashed.indexOf("b")).isEqualTo(1);
		Assertions.assertThat(hashed.indexOf("c")).isEqualTo(-1);

		Assertions.assertThat(hashed.unsafeIndexOf("a")).isEqualTo(0);
		Assertions.assertThat(hashed.unsafeIndexOf("b")).isEqualTo(1);
		Assertions.assertThat(hashed.unsafeIndexOf("c")).isEqualTo(0);
		Assertions.assertThat(hashed.unsafeIndexOf("d")).isEqualTo(1);

		Assertions.assertThat(hashed)
				.isInstanceOfSatisfying(SimplePerfectHash.class, hh -> Assertions.assertThat(hh.remapped).hasSize(2));
	}

	@Test
	public void testHash_3() {
		IHasIndexOf<String> hashed = SimplePerfectHash.make(List.of("a", "b", "c"));
		Assertions.assertThat(hashed.indexOf("a")).isEqualTo(0);
		Assertions.assertThat(hashed.indexOf("b")).isEqualTo(1);
		Assertions.assertThat(hashed.indexOf("c")).isEqualTo(2);

		Assertions.assertThat(hashed.unsafeIndexOf("a")).isEqualTo(0);
		Assertions.assertThat(hashed.unsafeIndexOf("b")).isEqualTo(1);
		Assertions.assertThat(hashed.unsafeIndexOf("c")).isEqualTo(2);
		Assertions.assertThat(hashed.unsafeIndexOf("d")).isEqualTo(-1);

		Assertions.assertThat(hashed)
				.isInstanceOfSatisfying(SimplePerfectHash.class, hh -> Assertions.assertThat(hh.remapped).hasSize(4));
	}

	@Test
	public void testHash_4() {
		IHasIndexOf<String> hashed = SimplePerfectHash.make(List.of("a", "b", "c", "d"));
		Assertions.assertThat(hashed.indexOf("a")).isEqualTo(0);
		Assertions.assertThat(hashed.indexOf("b")).isEqualTo(1);
		Assertions.assertThat(hashed.indexOf("c")).isEqualTo(2);

		Assertions.assertThat(hashed.unsafeIndexOf("a")).isEqualTo(0);
		Assertions.assertThat(hashed.unsafeIndexOf("b")).isEqualTo(1);
		Assertions.assertThat(hashed.unsafeIndexOf("c")).isEqualTo(2);
		Assertions.assertThat(hashed.unsafeIndexOf("d")).isEqualTo(3);

		Assertions.assertThat(hashed)
				.isInstanceOfSatisfying(SimplePerfectHash.class, hh -> Assertions.assertThat(hh.remapped).hasSize(4));
	}

	/**
	 * 
	 * @param size
	 * @param breakOnConlict
	 *            can be turned to false for some fuzzy tests
	 * @return a {@link SimplePerfectHash} which required given remapped length
	 */
	private SimplePerfectHash<String> lookForConflict(int size, int remappedLengthForBreak) {
		log.info("seed={}", seed);
		Random r = new Random(seed);

		while (true) {
			List<String> input = IntStream.range(0, size).mapToObj(i -> Long.toString(r.nextLong())).toList();

			SimplePerfectHash<String> hashed = (SimplePerfectHash<String>) SimplePerfectHash.make(input);

			int[] indexes = IntStream.range(0, size).map(i -> hashed.indexOf(input.get(i))).toArray();

			Assertions.assertThat(indexes).contains(IntStream.range(0, size).toArray());

			if (hashed.remapped.length > size) {
				log.info("We observed a conflict (size={}) for {}", hashed.remapped.length, input);

				return hashed;
			} else {
				log.info("OK");
			}
		}
	}

	@Test
	public void testHash_4_randomUntilConflict() {
		int size = 4;

		SimplePerfectHash<String> conflicting = lookForConflict(size, size);

		// ensure we properly manage in case of conflict
		Assertions.assertThat(conflicting.remapped.length).isGreaterThanOrEqualTo(8);
	}

	@Test
	public void testHash_16_randomUntilConflict() {
		int size = 16;

		lookForConflict(size, size);
	}

	@Disabled
	@Test
	public void testHash_16_randomUntilConflict_breakOnLarge() {
		int size = 16;

		lookForConflict(size, size * 1024);
	}

	// This was leading to a large table before enabling shifting
	@Test
	public void testHash_16_problematicEntry() {
		// An input from testHash_16_randomUntilConflict_breakOnLarge having led to a very problematic input
		String rawInput =
				"-32793283763807, -2667803750795669621, -2686441745666743748, 2210319168105504753, -2206126985636351926, 8416153826711128749, -6043220469414065009, -2696012513245280177, 4896350160836047244, -4565051295721678311, -3377919030986978560, 7056362477470480171, -6150347870998093391, 6537526290760067039, 300938195559530347, 1118132551240414070";
		List<String> problematicInput = Arrays.asList(rawInput.split(", "));

		SimplePerfectHash.make(problematicInput);
	}

	// This was leading to a large table before enabling shifting
	@Test
	public void testHash_2_problematicEntry() {
		// These 2 entries are very similar given their hashCode bit representation
		List<String> problematicInput = List.of("-32793283763807", "-2206126985636351926");

		Assertions.assertThat(problematicInput.get(0).hashCode()).isEqualTo(1_920_519_763);
		Assertions.assertThat(Strings.padStart(Integer.toBinaryString(problematicInput.get(0).hashCode()), 32, '0'))
				.isEqualTo("01110010011110001100111001010011");
		Assertions.assertThat(problematicInput.get(1).hashCode()).isEqualTo(-226_963_885);
		Assertions.assertThat(Strings.padStart(Integer.toBinaryString(problematicInput.get(1).hashCode()), 32, '0'))
				.isEqualTo("11110010011110001100111001010011");

		IHasIndexOf<String> hashed = SimplePerfectHash.make(problematicInput);
		Assertions.assertThat(hashed.indexOf("-32793283763807")).isEqualTo(0);
		Assertions.assertThat(hashed.indexOf("-2206126985636351926")).isEqualTo(1);
		Assertions.assertThat(hashed.indexOf("c")).isEqualTo(-1);

		Assertions.assertThat(hashed.unsafeIndexOf("-32793283763807")).isEqualTo(0);
		Assertions.assertThat(hashed.unsafeIndexOf("-2206126985636351926")).isEqualTo(1);
		Assertions.assertThat(hashed.unsafeIndexOf("c")).isEqualTo(0);

		Assertions.assertThat(hashed)
				.isInstanceOfSatisfying(SimplePerfectHash.class, hh -> Assertions.assertThat(hh.remapped).hasSize(2));
	}

	@Test
	public void testHashcodeConflict() {
		long l1 = 1;
		long l2 = 4294967296L;
		long l3 = 8589934595L;

		List<Long> problematicInput = List.of(l1, l2, l3);
		IHasIndexOf<Long> hashed = SimplePerfectHash.make(problematicInput);

		Assertions.assertThat(hashed.indexOf(l1)).isEqualTo(0);
		Assertions.assertThat(hashed.indexOf(l2)).isEqualTo(1);
		Assertions.assertThat(hashed.indexOf(l3)).isEqualTo(2);

		Assertions.assertThat(hashed).isInstanceOf(HashMapIndexOf.class);

	}
}
