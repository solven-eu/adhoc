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
package eu.solven.adhoc.encoding.bytes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.pepper.unittest.PepperJacksonTestHelper;

public class TestUtf8ByteSlice {
	@Test
	public void testUtf8ByteSlice() {
		Utf8ByteSlice slice = Utf8ByteSlice.fromString("foo");

		Assertions.assertThat(slice).hasToString("foo");
	}

	@Test
	public void testJackson() {
		Utf8ByteSlice original = Utf8ByteSlice.fromString("foo");
		String asString = PepperJacksonTestHelper.asString(Object.class, original);

		Assertions.assertThat(asString).isEqualTo("\"foo\"");
	}

	@Test
	public void testEquals_sameContent_differentInstances() {
		// Distinct Utf8ByteSlice instances built from the same String must be equal.
		Utf8ByteSlice a = Utf8ByteSlice.fromString("foo");
		Utf8ByteSlice b = Utf8ByteSlice.fromString("foo");

		Assertions.assertThat(a).isNotSameAs(b).isEqualTo(b);
		Assertions.assertThat(a.hashCode()).isEqualTo(b.hashCode());
	}

	@Test
	public void testEquals_differentContent() {
		Utf8ByteSlice a = Utf8ByteSlice.fromString("foo");
		Utf8ByteSlice b = Utf8ByteSlice.fromString("bar");

		Assertions.assertThat(a).isNotEqualTo(b);
	}

	@Test
	public void testEquals_self() {
		Utf8ByteSlice a = Utf8ByteSlice.fromString("foo");
		Assertions.assertThat(a).isEqualTo(a);
	}

	@Test
	public void testEquals_null() {
		Utf8ByteSlice a = Utf8ByteSlice.fromString("foo");
		Assertions.assertThat(a).isNotEqualTo(null);
	}

	@Test
	public void testEquals_notEqualToString() {
		// A Utf8ByteSlice carrying "foo" is NOT equal to the String "foo": it is a byte-level value, not a UTF-16
		// String. Mixing them in a Set/Map would otherwise produce surprising lookup results.
		Utf8ByteSlice a = Utf8ByteSlice.fromString("foo");
		Assertions.assertThat((Object) a).isNotEqualTo("foo");
		Assertions.assertThat((Object) "foo").isNotEqualTo(a);
	}

	@Test
	public void testEquals_notEqualToRawByteSlice() {
		// Equality is restricted to other Utf8ByteSlice instances: a raw IByteSlice carrying the same bytes
		// is intentionally NOT equal, because the Utf8 marker is part of the type identity.
		Utf8ByteSlice utf8 = Utf8ByteSlice.fromString("foo");
		IByteSlice raw = IByteSlice.wrap("foo".getBytes(java.nio.charset.StandardCharsets.UTF_8));

		Assertions.assertThat((Object) utf8).isNotEqualTo(raw);
	}

	@Test
	public void testHashCode_independentFromStringHashCode() {
		// The hash is byte-derived (31*h+b loop, seeded at 1 — the IByteSlice convention), NOT
		// String.hashCode (which iterates over UTF-16 code units, seeded at 0). They MUST NOT be assumed to match.
		// If a future implementation switches to a seed/loop that happens to coincide with String.hashCode for ASCII,
		// that is opportunistic — callers MUST NOT rely on it.
		Utf8ByteSlice a = Utf8ByteSlice.fromString("hello");
		Assertions.assertThat(a.hashCode()).isNotEqualTo("hello".hashCode());

		String accented = "café";
		Utf8ByteSlice b = Utf8ByteSlice.fromString(accented);
		Assertions.assertThat(b.hashCode()).isNotEqualTo(accented.hashCode());
	}

	@Test
	public void testHashSet_dedupsContentEquivalentInstances() {
		// Practical consequence: a HashSet/HashMap deduplicates content-equivalent Utf8ByteSlice instances. This
		// is what enables DistinctFreezer to actually see low cardinality on Arrow-sourced columns where every
		// row is wrapped in a fresh Utf8ByteSlice instance.
		java.util.Set<Utf8ByteSlice> set = new java.util.HashSet<>();
		for (int i = 0; i < 64; i++) {
			set.add(Utf8ByteSlice.fromString(i % 2 == 0 ? "foo" : "bar"));
		}

		Assertions.assertThat(set).hasSize(2);
	}
}
