/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.map.factory;

import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;

public class TestSequencedSetLikeList {
	@Disabled("TODO")
	@Test
	public void testHashcodeEquals() {
		EqualsVerifier.forClass(SequencedSetLikeList.class)
				.suppress(Warning.STRICT_INHERITANCE)
				// .suppress(Warning.REFERENCE_EQUALITY)
				.withIgnoredFields("reordering")
				.verify();
	}

	@Test
	public void testSequence2() {
		SequencedSetLikeList sequence = SequencedSetLikeList.fromSet(ImmutableSet.of("b", "a"));

		Assertions.assertThat(sequence.getKey(0)).isEqualTo("b");
		Assertions.assertThat(sequence.getKey(1)).isEqualTo("a");

		Assertions.assertThat(sequence.indexOf("b")).isEqualTo(0);
		Assertions.assertThat(sequence.indexOf("a")).isEqualTo(1);

		Assertions.assertThat((Set) sequence).hasToString("[b, a]");
	}

	@Test
	public void testSequence3() {
		SequencedSetLikeList sequence = SequencedSetLikeList.fromSet(ImmutableSet.of("b", "c", "a"));

		Assertions.assertThat(sequence.getKey(0)).isEqualTo("b");
		Assertions.assertThat(sequence.getKey(1)).isEqualTo("c");
		Assertions.assertThat(sequence.getKey(2)).isEqualTo("a");

		Assertions.assertThat(sequence.indexOf("b")).isEqualTo(0);
		Assertions.assertThat(sequence.indexOf("c")).isEqualTo(1);
		Assertions.assertThat(sequence.indexOf("a")).isEqualTo(2);

		Assertions.assertThat((Set) sequence).hasToString("[b, c, a]");
	}

	@Test
	public void testSequence_overNavigableMap() {
		SequencedSetLikeList sequence = SequencedSetLikeList.fromSet(ImmutableSortedSet.of("a", "b", "c"));

		Assertions.assertThat(sequence.getKey(0)).isEqualTo("a");
		Assertions.assertThat(sequence.getKey(1)).isEqualTo("b");
		Assertions.assertThat(sequence.getKey(2)).isEqualTo("c");

		Assertions.assertThat(sequence.indexOf("a")).isEqualTo(0);
		Assertions.assertThat(sequence.indexOf("b")).isEqualTo(1);
		Assertions.assertThat(sequence.indexOf("c")).isEqualTo(2);

		Assertions.assertThat((Set) sequence).hasToString("[a, b, c]");
	}

	@Test
	public void testHashCodeEquals_differentOrders() {
		SequencedSetLikeList abc = SequencedSetLikeList.fromSet(ImmutableSet.of("a", "b", "c"));
		SequencedSetLikeList bca = SequencedSetLikeList.fromSet(ImmutableSet.of("b", "c", "a"));
		SequencedSetLikeList cab = SequencedSetLikeList.fromSet(ImmutableSet.of("c", "a", "b"));

		Assertions.assertThat((Set) abc).isEqualTo(bca).hasSameHashCodeAs(bca).isEqualTo(cab).hasSameHashCodeAs(cab);
		Assertions.assertThat(abc.asList()).isEqualTo(List.of("a", "b", "c")).hasSameHashCodeAs(List.of("a", "b", "c"));
	}

	@Test
	public void testAsList() {
		SequencedSetLikeList abc = SequencedSetLikeList.fromSet(ImmutableSet.of("a", "c", "b"));
		List<String> asList = abc.asList();
		List<String> rawList = List.of("a", "c", "b");

		Assertions.assertThat(asList).isEqualTo(rawList).hasSameHashCodeAs(rawList);
		Assertions.assertThat(asList).element(0).isEqualTo("a");
		Assertions.assertThat(asList).element(1).isEqualTo("c");
		Assertions.assertThat(asList).element(2).isEqualTo("b");
	}
}
