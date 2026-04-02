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
package eu.solven.adhoc.util;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.collection.AdhocCollectionHelpers;

public class TestAdhocCollectionHelpers {
	LocalDate now = LocalDate.now();

	@Test
	public void testUnnestCollection_noNested() {
		List<Object> noNested = Arrays.asList(123, 12.34, "foo", now);
		Assertions.assertThat(AdhocCollectionHelpers.unnestAsCollection(noNested)).isEqualTo(noNested);
	}

	@Test
	public void testUnnestEmpty() {
		Assertions.assertThat(AdhocCollectionHelpers.unnestAsCollection(List.of())).isEmpty();
		Assertions.assertThat(AdhocCollectionHelpers.unnestAsCollection(List.of(List.of()))).isEmpty();
		Assertions.assertThat(AdhocCollectionHelpers.unnestAsCollection(List.of(List.of(List.of())))).isEmpty();
	}

	@Test
	public void testUnnestSingleton() {
		Assertions.assertThat((Collection) AdhocCollectionHelpers.unnestAsCollection(List.of("a")))
				.containsExactly("a");
		Assertions.assertThat((Collection) AdhocCollectionHelpers.unnestAsCollection(List.of(List.of("a"))))
				.containsExactly("a");
		Assertions.assertThat((Collection) AdhocCollectionHelpers.unnestAsCollection(List.of(List.of(List.of("a")))))
				.containsExactly("a");
	}

	@Test
	public void testUnnestCollection_variousDepth() {
		List<Object> noNested = Arrays.asList(123, Arrays.asList(12.34, Arrays.asList(Arrays.asList("foo"), now)));
		Assertions.assertThat(AdhocCollectionHelpers.unnestAsCollection(noNested))
				.isEqualTo(Arrays.asList(123, 12.34D, "foo", now));
	}

	@Test
	public void testUnnestCollection_null() {
		List<Object> noNested = Arrays.asList(123, null, Arrays.asList(null, "foo"));
		Assertions.assertThat(AdhocCollectionHelpers.unnestAsCollection(noNested))
				.isEqualTo(Arrays.asList(123, null, null, "foo"));
	}

	@Test
	public void testUnnestList_null() {
		List<Object> noNested = Arrays.asList(123, null, Arrays.asList(null, "foo"));
		Assertions.assertThat(AdhocCollectionHelpers.unnestAsList(noNested, Predicates.alwaysTrue()))
				.isEqualTo(Arrays.asList(123, null, null, "foo"));
	}

	@Test
	public void testCartesianProduct() {
		Assertions.assertThat(AdhocCollectionHelpers.cartesianProductSize(List.of())).isEqualTo(BigInteger.ZERO);

		List<List<?>> size5 = IntStream.range(0, 5)
				.mapToObj(i -> List.of("a", "b", "c"))
				.collect(Collectors.toCollection(ArrayList::new));
		Assertions.assertThat(AdhocCollectionHelpers.cartesianProductSize(size5))
				.isEqualTo(BigInteger.valueOf(3 * 3 * 3 * 3 * 3));

		// Does not even fit in a long
		List<List<?>> size1024 = IntStream.range(0, 128)
				.mapToObj(i -> List.of("a", "b", "c"))
				.collect(Collectors.toCollection(ArrayList::new));
		Assertions.assertThat(AdhocCollectionHelpers.cartesianProductSize(size1024))
				.hasToString("11790184577738583171520872861412518665678211592275841109096961");
	}

	@Test
	public void testCopyOfSets() {
		Assertions.assertThat(AdhocCollectionHelpers.copyOfSets(Set.of(), Set.of("foo"))).containsExactly("foo");
		Assertions.assertThat(AdhocCollectionHelpers.copyOfSets(Set.of("foo"), Set.of())).containsExactly("foo");
		Assertions.assertThat(AdhocCollectionHelpers.copyOfSets(Set.of("foo"), Set.of("bar")))
				.containsExactly("foo", "bar");
	}

	@Test
	public void testTrimToSize() {
		AdhocCollectionHelpers.trimToSize(List.of("foo"));
		AdhocCollectionHelpers.trimToSize(new ArrayList<>(List.of("foo")));
		AdhocCollectionHelpers.trimToSize(ImmutableList.of("foo"));
	}

	@Test
	public void testGetFirst() {
		Assertions.assertThat(AdhocCollectionHelpers.getFirst(List.of("foo"))).isEqualTo("foo");
		Assertions.assertThat(AdhocCollectionHelpers.getFirst(Set.of("foo"))).isEqualTo("foo");

		Assertions.assertThat(AdhocCollectionHelpers.getFirst(ImmutableList.of("foo"))).isEqualTo("foo");
		Assertions.assertThat(AdhocCollectionHelpers.getFirst(ImmutableSet.of("foo"))).isEqualTo("foo");

		Assertions.assertThatThrownBy(() -> AdhocCollectionHelpers.getFirst(Set.of()))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	@PerformanceGateway
	public void testUnsetLargeSingleCollection() {
		int size = 16 * 1024;
		Assertions.assertThat(AdhocCollectionHelpers.unnestAsCollection(ContiguousSet.closedOpen(0, size)))
				.hasSize(size);
	}
}
