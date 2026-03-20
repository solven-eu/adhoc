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
package eu.solven.adhoc.encoding.perfect_hashing;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCollectionWithCustomIndexOf {

	// ── hashMap strategy ────────────────────────────────────────────────────

	@Test
	public void testHashMap_indexOf() {
		CollectionWithCustomIndexOf<String> col =
				CollectionWithCustomIndexOf.<String>builder().keys(List.of("a", "b", "c")).hashMap().build();

		Assertions.assertThat(col.indexOf("a")).isEqualTo(0);
		Assertions.assertThat(col.indexOf("b")).isEqualTo(1);
		Assertions.assertThat(col.indexOf("c")).isEqualTo(2);
	}

	@Test
	public void testHashMap_unknownKey() {
		CollectionWithCustomIndexOf<String> col =
				CollectionWithCustomIndexOf.<String>builder().keys(List.of("a", "b")).hashMap().build();

		Assertions.assertThat(col.indexOf("z")).isEqualTo(-1);
	}

	@Test
	public void testHashMap_getKeys() {
		List<String> keys = List.of("x", "y");
		CollectionWithCustomIndexOf<String> col =
				CollectionWithCustomIndexOf.<String>builder().keys(keys).hashMap().build();

		Assertions.assertThat(col.getKeys()).containsExactlyElementsOf(keys);
	}

	// ── default strategy (no explicit factory → hashMap) ────────────────────

	@Test
	public void testDefault_usesHashMap() {
		CollectionWithCustomIndexOf<String> col =
				CollectionWithCustomIndexOf.<String>builder().keys(List.of("p", "q")).build();

		Assertions.assertThat(col.indexOf("p")).isEqualTo(0);
		Assertions.assertThat(col.indexOf("q")).isEqualTo(1);
		Assertions.assertThat(col.indexOf("r")).isEqualTo(-1);
	}

	// ── perfectHash strategy ─────────────────────────────────────────────────

	@Test
	public void testPerfectHash_indexOf() {
		CollectionWithCustomIndexOf<String> col =
				CollectionWithCustomIndexOf.<String>builder().keys(List.of("a", "b", "c")).perfectHash().build();

		Assertions.assertThat(col.indexOf("a")).isEqualTo(0);
		Assertions.assertThat(col.indexOf("b")).isEqualTo(1);
		Assertions.assertThat(col.indexOf("c")).isEqualTo(2);
	}
}
