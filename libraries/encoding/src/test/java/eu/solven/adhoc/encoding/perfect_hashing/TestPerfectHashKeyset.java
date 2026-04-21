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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SequencedSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.util.immutable.UnsupportedAsImmutableException;

/**
 * One test per public method on {@link PerfectHashKeyset}.
 *
 * @author Benoit Lacelle
 */
public class TestPerfectHashKeyset {

	private static PerfectHashKeyset keyset() {
		return PerfectHashKeyset.of(List.of("alpha", "beta", "gamma"));
	}

	// ── of / getKeyFields ───────────────────────────────────────────────────

	@Test
	public void testOf_copiesIntoImmutableList() {
		List<String> mutable = new ArrayList<>(List.of("a", "b"));
		PerfectHashKeyset keys = PerfectHashKeyset.of(mutable);

		Assertions.assertThat(keys.getKeyFields()).containsExactly("a", "b").isInstanceOf(ImmutableList.class);

		// Mutating the caller's list does not leak into the keyset.
		mutable.add("c");
		Assertions.assertThat(keys.getKeyFields()).containsExactly("a", "b");
	}

	@Test
	public void testOf_empty() {
		PerfectHashKeyset keys = PerfectHashKeyset.of(List.of());
		Assertions.assertThat(keys.getKeyFields()).isEmpty();
		Assertions.assertThat(keys).isEmpty();
	}

	// ── indexOf / unsafeIndexOf ─────────────────────────────────────────────

	@Test
	public void testIndexOf() {
		PerfectHashKeyset keys = keyset();
		Assertions.assertThat(keys.indexOf("alpha")).isZero();
		Assertions.assertThat(keys.indexOf("beta")).isEqualTo(1);
		Assertions.assertThat(keys.indexOf("gamma")).isEqualTo(2);
		Assertions.assertThat(keys.indexOf("missing")).isEqualTo(-1);
	}

	@Test
	public void testUnsafeIndexOf_matchesIndexOfForPresentKeys() {
		PerfectHashKeyset keys = keyset();

		// For present keys, unsafeIndexOf returns the same slot as indexOf.
		Assertions.assertThat(keys.unsafeIndexOf("alpha")).isEqualTo(keys.indexOf("alpha"));
		Assertions.assertThat(keys.unsafeIndexOf("beta")).isEqualTo(keys.indexOf("beta"));
		Assertions.assertThat(keys.unsafeIndexOf("gamma")).isEqualTo(keys.indexOf("gamma"));
	}

	// ── size / isEmpty ──────────────────────────────────────────────────────

	@Test
	public void testSize() {
		Assertions.assertThat(keyset().size()).isEqualTo(3);
		Assertions.assertThat(PerfectHashKeyset.of(List.of()).size()).isZero();
	}

	@Test
	public void testIsEmpty() {
		Assertions.assertThat(keyset().isEmpty()).isFalse();
		Assertions.assertThat(PerfectHashKeyset.of(List.of()).isEmpty()).isTrue();
	}

	// ── contains / containsAll ──────────────────────────────────────────────

	@Test
	public void testContains() {
		PerfectHashKeyset keys = keyset();
		Assertions.assertThat(keys.contains("alpha")).isTrue();
		Assertions.assertThat(keys.contains("missing")).isFalse();
		// Non-String key: must return false, not throw.
		Assertions.assertThat(keys.contains(42)).isFalse();
		Assertions.assertThat(keys.contains(null)).isFalse();
	}

	@Test
	public void testContainsAll() {
		PerfectHashKeyset keys = keyset();
		Assertions.assertThat(keys.containsAll(List.of("alpha", "gamma"))).isTrue();
		Assertions.assertThat(keys.containsAll(List.of("alpha", "missing"))).isFalse();
		// Empty collection is always contained.
		Assertions.assertThat(keys.containsAll(List.of())).isTrue();
		// A non-String element forces the early false branch.
		Assertions.assertThat(keys.containsAll(List.of("alpha", 42))).isFalse();
	}

	// ── iterator / spliterator / forEach / toArray ──────────────────────────

	@Test
	public void testIterator_walksInDeclaredOrder() {
		Iterator<String> it = keyset().iterator();
		Assertions.assertThat(it.hasNext()).isTrue();
		Assertions.assertThat(it.next()).isEqualTo("alpha");
		Assertions.assertThat(it.next()).isEqualTo("beta");
		Assertions.assertThat(it.next()).isEqualTo("gamma");
		Assertions.assertThat(it.hasNext()).isFalse();
	}

	@Test
	public void testSpliterator_reportsDeclaredOrder() {
		Spliterator<String> sp = keyset().spliterator();
		List<String> drained = StreamSupport.stream(sp, false).collect(Collectors.toList());
		Assertions.assertThat(drained).containsExactly("alpha", "beta", "gamma");
	}

	@Test
	public void testForEach_visitsInDeclaredOrder() {
		List<String> visited = new ArrayList<>();
		keyset().forEach(visited::add);
		Assertions.assertThat(visited).containsExactly("alpha", "beta", "gamma");
	}

	@Test
	public void testToArray_objectArray() {
		Object[] array = keyset().toArray();
		Assertions.assertThat(array).containsExactly("alpha", "beta", "gamma");
	}

	@Test
	public void testToArray_typedArray() {
		// Typed variant: caller passes a pre-sized or zero-sized array — the returned array is either the input
		// (filled) or a new one of the same runtime type.
		String[] typed = keyset().toArray(new String[0]);
		Assertions.assertThat(typed).containsExactly("alpha", "beta", "gamma");
	}

	// ── getFirst / getLast ──────────────────────────────────────────────────

	@Test
	public void testGetFirst_getLast() {
		PerfectHashKeyset keys = keyset();
		Assertions.assertThat(keys.getFirst()).isEqualTo("alpha");
		Assertions.assertThat(keys.getLast()).isEqualTo("gamma");
	}

	@Test
	public void testGetFirst_emptyThrows() {
		// `ImmutableList.get(0)` on an empty list throws IndexOutOfBoundsException — we propagate it unchanged to
		// stay consistent with `List.getFirst()` / `SequencedSet.getFirst()` behaviour on other empty sequences.
		PerfectHashKeyset empty = PerfectHashKeyset.of(List.of());
		Assertions.assertThatThrownBy(empty::getFirst)
				.isInstanceOfAny(IndexOutOfBoundsException.class, NoSuchElementException.class);
	}

	@Test
	public void testGetLast_emptyThrows() {
		PerfectHashKeyset empty = PerfectHashKeyset.of(List.of());
		Assertions.assertThatThrownBy(empty::getLast)
				.isInstanceOfAny(IndexOutOfBoundsException.class, NoSuchElementException.class);
	}

	// ── reversed ────────────────────────────────────────────────────────────

	@Test
	public void testReversed_preservesReverseOrder() {
		SequencedSet<String> reversed = keyset().reversed();
		Assertions.assertThat(reversed).containsExactly("gamma", "beta", "alpha");
		Assertions.assertThat(reversed.getFirst()).isEqualTo("gamma");
		Assertions.assertThat(reversed.getLast()).isEqualTo("alpha");
	}

	@Test
	public void testReversed_isImmutable() {
		SequencedSet<String> reversed = keyset().reversed();
		Assertions.assertThatThrownBy(() -> reversed.add("delta")).isInstanceOf(UnsupportedOperationException.class);
	}

	// ── equals / hashCode / toString ────────────────────────────────────────

	@Test
	public void testEquals_sameReference() {
		PerfectHashKeyset keys = keyset();
		Assertions.assertThat(keys).isEqualTo(keys);
	}

	@Test
	public void testEquals_sameContent_viaOrdinarySet() {
		// Set contract: equality ignores iteration order and concrete class — only the element set matters.
		PerfectHashKeyset keys = keyset();
		Set<String> reference = new LinkedHashSet<>(List.of("gamma", "alpha", "beta"));
		Assertions.assertThat(keys).isEqualTo(reference);
		Assertions.assertThat(keys.hashCode()).isEqualTo(reference.hashCode());
	}

	@Test
	public void testEquals_differentSize() {
		Assertions.assertThat(keyset()).isNotEqualTo(PerfectHashKeyset.of(List.of("alpha", "beta")));
	}

	@Test
	public void testEquals_differentContent() {
		Assertions.assertThat(keyset()).isNotEqualTo(PerfectHashKeyset.of(List.of("alpha", "beta", "delta")));
	}

	@Test
	public void testEquals_nonSet() {
		PerfectHashKeyset keys = keyset();
		Assertions.assertThat(keys).isNotEqualTo(List.of("alpha", "beta", "gamma"));
		Assertions.assertThat(keys).isNotEqualTo("not a set");
		Assertions.assertThat(keys).isNotEqualTo(null);
	}

	@Test
	public void testEquals_setWithIncompatibleElements() {
		PerfectHashKeyset keys = keyset();
		// A Set<Integer> of the same size but whose containsAll call could ClassCastException — the implementation
		// catches CCE and returns false rather than throwing.
		Assertions.assertThat(keys).isNotEqualTo(Set.of(1, 2, 3));
	}

	@Test
	public void testHashCode_matchesSetContract() {
		PerfectHashKeyset keys = keyset();
		// Set contract: hashCode is the sum of element hashCodes.
		int expected = "alpha".hashCode() + "beta".hashCode() + "gamma".hashCode();
		Assertions.assertThat(keys.hashCode()).isEqualTo(expected);
	}

	@Test
	public void testToString_matchesKeyFields() {
		PerfectHashKeyset keys = keyset();
		Assertions.assertThat(keys).hasToString(List.of("alpha", "beta", "gamma").toString());
	}

	// ── Mutating operations are unsupported ────────────────────────────────

	@Test
	public void testAdd_unsupported() {
		Assertions.assertThatThrownBy(() -> keyset().add("delta")).isInstanceOf(UnsupportedAsImmutableException.class);
	}

	@Test
	public void testRemove_unsupported() {
		Assertions.assertThatThrownBy(() -> keyset().remove("alpha"))
				.isInstanceOf(UnsupportedAsImmutableException.class);
	}

	@Test
	public void testAddAll_unsupported() {
		Assertions.assertThatThrownBy(() -> keyset().addAll(List.of("delta", "epsilon")))
				.isInstanceOf(UnsupportedAsImmutableException.class);
	}

	@Test
	public void testRetainAll_unsupported() {
		Assertions.assertThatThrownBy(() -> keyset().retainAll(List.of("alpha")))
				.isInstanceOf(UnsupportedAsImmutableException.class);
	}

	@Test
	public void testRemoveAll_unsupported() {
		Assertions.assertThatThrownBy(() -> keyset().removeAll(List.of("alpha")))
				.isInstanceOf(UnsupportedAsImmutableException.class);
	}

	@Test
	public void testClear_unsupported() {
		Assertions.assertThatThrownBy(() -> keyset().clear()).isInstanceOf(UnsupportedAsImmutableException.class);
	}
}
