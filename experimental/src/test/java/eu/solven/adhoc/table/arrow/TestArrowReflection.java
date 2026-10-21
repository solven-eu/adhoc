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
package eu.solven.adhoc.table.arrow;

import java.math.BigInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The grouping indicator may be materialized by Arrow as any integral type: the absence check must not depend on the
 * exact boxed class.
 */
public class TestArrowReflection {
	@Test
	public void testIsAbsentFromGroupingSet_long() {
		Assertions.assertThat(ArrowReflection.isAbsentFromGroupingSet(Long.valueOf(0))).isFalse();
		Assertions.assertThat(ArrowReflection.isAbsentFromGroupingSet(Long.valueOf(1))).isTrue();
	}

	@Test
	public void testIsAbsentFromGroupingSet_int() {
		// Arrow may return an `Int` vector instead of a `BigInt` one
		Assertions.assertThat(ArrowReflection.isAbsentFromGroupingSet(Integer.valueOf(0))).isFalse();
		Assertions.assertThat(ArrowReflection.isAbsentFromGroupingSet(Integer.valueOf(1))).isTrue();
	}

	@Test
	public void testIsAbsentFromGroupingSet_smallerIntegrals() {
		Assertions.assertThat(ArrowReflection.isAbsentFromGroupingSet(Short.valueOf((short) 0))).isFalse();
		Assertions.assertThat(ArrowReflection.isAbsentFromGroupingSet(Short.valueOf((short) 1))).isTrue();
		Assertions.assertThat(ArrowReflection.isAbsentFromGroupingSet(Byte.valueOf((byte) 0))).isFalse();
		Assertions.assertThat(ArrowReflection.isAbsentFromGroupingSet(Byte.valueOf((byte) 1))).isTrue();
	}

	@Test
	public void testIsAbsentFromGroupingSet_bigInteger() {
		Assertions.assertThat(ArrowReflection.isAbsentFromGroupingSet(BigInteger.ZERO)).isFalse();
		Assertions.assertThat(ArrowReflection.isAbsentFromGroupingSet(BigInteger.ONE)).isTrue();
	}

	@Test
	public void testIsAbsentFromGroupingSet_notANumber() {
		// Anything which is not a numeric `0` is considered absent
		Assertions.assertThat(ArrowReflection.isAbsentFromGroupingSet(null)).isTrue();
		Assertions.assertThat(ArrowReflection.isAbsentFromGroupingSet("0")).isTrue();
	}
}
