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
package eu.solven.adhoc.dataframe.aggregating;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import it.unimi.dsi.fastutil.objects.Object2IntMap;

public class TestAdhocPrimitiveMapHelpers {

	@Test
	public void testDefaultReturnValue_missingKey() {
		Object2IntMap<String> map = AdhocPrimitiveMapHelpers.newHashMapDefaultMinus1();

		Assertions.assertThat(map.getInt("absent")).isEqualTo(-1);
	}

	@Test
	public void testDefaultReturnValue_presentKey() {
		Object2IntMap<String> map = AdhocPrimitiveMapHelpers.newHashMapDefaultMinus1();

		map.put("k", 42);

		Assertions.assertThat(map.getInt("k")).isEqualTo(42);
		// value 0 must not be confused with any sentinel
		map.put("zero", 0);
		Assertions.assertThat(map.getInt("zero")).isEqualTo(0);
	}

	/**
	 * Verifies that a map pre-sized for 256 entries can hold exactly 256 entries without loss, and that the default
	 * return value of -1 is preserved throughout — including after any internal rehash that the implementation may
	 * trigger.
	 *
	 * <p>
	 * {@link it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap} treats the constructor argument as the expected
	 * number of entries. For capacity=256 with the default load factor of 0.75, the backing table is sized to 512 slots
	 * (maxFill=384), so 256 insertions must not trigger a rehash.
	 */
	@Test
	public void testCapacity256_size() {
		Object2IntMap<String> map = AdhocPrimitiveMapHelpers.newHashMapDefaultMinus1(256);

		Assertions.assertThat(map).isEmpty();
		Assertions.assertThat(map.getInt("absent")).isEqualTo(-1);

		for (int i = 0; i < 256; i++) {
			map.put("key_" + i, i);
		}

		Assertions.assertThat(map).hasSize(256);
		Assertions.assertThat(map.getInt("key_0")).isEqualTo(0);
		Assertions.assertThat(map.getInt("key_255")).isEqualTo(255);
		// default -1 must still work for missing keys after all insertions
		Assertions.assertThat(map.getInt("absent")).isEqualTo(-1);
	}

	/**
	 * Edge case: capacity=0 must produce a usable map (fastutil allows this and sizes the table to its minimum of 2
	 * slots).
	 */
	@Test
	public void testCapacity0() {
		Object2IntMap<String> map = AdhocPrimitiveMapHelpers.newHashMapDefaultMinus1(0);

		Assertions.assertThat(map).isEmpty();
		Assertions.assertThat(map.getInt("absent")).isEqualTo(-1);

		map.put("k", 7);

		Assertions.assertThat(map).hasSize(1);
		Assertions.assertThat(map.getInt("k")).isEqualTo(7);
	}

	/** Value -1 stored explicitly must be retrievable and not confused with the default. */
	@Test
	public void testStoredMinusOne() {
		Object2IntMap<String> map = AdhocPrimitiveMapHelpers.newHashMapDefaultMinus1();

		map.put("neg", -1);

		Assertions.assertThat(map.getInt("neg")).isEqualTo(-1);
		Assertions.assertThat(map.containsKey("neg")).isTrue();
		Assertions.assertThat(map.containsKey("absent")).isFalse();
	}
}
