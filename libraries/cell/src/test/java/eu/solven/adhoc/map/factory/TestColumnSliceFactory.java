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
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.encoding.column.freezer.AsynchronousFreezingStrategy;
import eu.solven.adhoc.encoding.page.AAppendableTable;
import eu.solven.adhoc.encoding.page.AppendableTableUnsafe;
import eu.solven.adhoc.filter.value.NullMatcher;
import eu.solven.adhoc.map.AdhocMapUnsafe;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.factory.ColumnSliceFactory.MapBuilderPreKeys;
import eu.solven.adhoc.options.StandardQueryOptions;

public class TestColumnSliceFactory {

	ColumnSliceFactory factory = ColumnSliceFactory.builder().build();

	@Test
	public void testRetainAll() {
		IAdhocMap aAndB = factory.newMapBuilder(List.of("a", "b")).append("a1").append("b1").build();

		IAdhocMap onlyA = aAndB.retainAll(Set.of("a"));
		Assertions.assertThat((Map) onlyA).isEqualTo(Map.of("a", "a1")).hasSameHashCodeAs(Map.of("a", "a1"));

		IAdhocMap onlyB = aAndB.retainAll(ImmutableSet.of("b"));
		Assertions.assertThat((Map) onlyB).isEqualTo(Map.of("b", "b1")).hasSameHashCodeAs(Map.of("b", "b1"));

		IAdhocMap retainAll = aAndB.retainAll(ImmutableSet.of("a", "b"));
		Assertions.assertThat((Map) retainAll).isSameAs(aAndB);
	}

	@Test
	public void testRetainNotIn() {
		IAdhocMap aAndB = factory.newMapBuilder(List.of("a", "b")).append("a1").append("b1").build();

		// disjoint
		IAdhocMap disjoint = aAndB.retainAll(Set.of("c"));
		Assertions.assertThat((Map) disjoint).isEqualTo(Map.of()).hasSameHashCodeAs(Map.of());

		// joint
		IAdhocMap joint = aAndB.retainAll(Set.of("a", "c"));
		Assertions.assertThat((Map) joint).isEqualTo(Map.of("a", "a1")).hasSameHashCodeAs(Map.of("a", "a1"));
	}

	@Test
	public void testBuild_DifferentOrder() {
		IAdhocMap aAndB = factory.newMapBuilder(List.of("a", "b")).append("a1", "b1").build();
		IAdhocMap bAndA = factory.newMapBuilder(List.of("b", "a")).append("b1", "a1").build();

		Assertions.assertThat((Map) aAndB).isEqualTo(bAndA).isEqualTo(ImmutableMap.of("a", "a1", "b", "b1"));
	}

	@Test
	public void testRetainAll_DifferentOrder() {
		IAdhocMap aAndBandC = factory.newMapBuilder(List.of("a", "b", "c")).append("a1", "b1", "c1").build();

		IAdhocMap retainAandB = aAndBandC.retainAll(ImmutableSet.of("b", "a"));

		Assertions.assertThat((Map) retainAandB).isEqualTo(ImmutableMap.of("a", "a1", "b", "b1"));
	}

	@Test
	public void testRetainAll_retainAllAgain() {
		IAdhocMap aAndBandC = factory.newMapBuilder(List.of("a", "b", "c")).append("a1", "b1", "c1").build();

		IAdhocMap retainAandB = aAndBandC.retainAll(ImmutableSet.of("a", "b"));
		IAdhocMap retainA = retainAandB.retainAll(ImmutableSet.of("a"));

		Assertions.assertThat((Map) retainA)
				.isEqualTo(ImmutableMap.of("a", "a1"))
				.hasSameHashCodeAs(ImmutableMap.of("a", "a1"));
	}

	@Test
	public void testRetainAll_retainAllAgain_retainAllAgain() {
		IAdhocMap aAndBandC = factory.newMapBuilder(List.of("a", "b", "c", "d")).append("a1", "b1", "c1", "d1").build();

		IAdhocMap retainAandBandC = aAndBandC.retainAll(ImmutableSet.of("a", "b", "c"));
		IAdhocMap retainAandB = retainAandBandC.retainAll(ImmutableSet.of("b", "c"));
		IAdhocMap retainA = retainAandB.retainAll(ImmutableSet.of("b"));

		Assertions.assertThat((Map) retainA)
				.isEqualTo(ImmutableMap.of("b", "b1"))
				.hasSameHashCodeAs(ImmutableMap.of("b", "b1"));
	}

	@Test
	public void testIssueWithCacheBasedOnSetInsteadOfList() {
		AdhocMapUnsafe.clearCaches();

		{
			IAdhocMap aAndB = factory.newMapBuilder(List.of("a", "b")).append("a1", "b1").build();
			IAdhocMap onlyAfromAandB = aAndB.retainAll(ImmutableSet.of("a"));

			Assertions.assertThat((Map) onlyAfromAandB)
					.isEqualTo(ImmutableMap.of("a", "a1"))
					.hasSameHashCodeAs(ImmutableMap.of("a", "a1"));
		}

		{
			IAdhocMap bAndA = factory.newMapBuilder(List.of("b", "a")).append("b1", "a1").build();
			IAdhocMap onlyAfromBandA = bAndA.retainAll(ImmutableSet.of("a"));

			Assertions.assertThat((Map) onlyAfromBandA)
					.isEqualTo(ImmutableMap.of("a", "a1"))
					.hasSameHashCodeAs(ImmutableMap.of("a", "a1"));
		}
	}

	// -------------------------------------------------------------------------
	// containsValue
	// -------------------------------------------------------------------------

	@Test
	public void testContainsValue_existing() {
		IAdhocMap aAndB = factory.newMapBuilder(List.of("a", "b")).append("a1").append("b1").build();

		Assertions.assertThat(aAndB.containsValue("a1")).isTrue();
		Assertions.assertThat(aAndB.containsValue("b1")).isTrue();
	}

	@Test
	public void testContainsValue_missing() {
		IAdhocMap aAndB = factory.newMapBuilder(List.of("a", "b")).append("a1").append("b1").build();

		Assertions.assertThat(aAndB.containsValue("z")).isFalse();
	}

	@Test
	public void testContainsValue_null_fromFailedJoin() {
		// null is normalized to NullMatcher.NULL_HOLDER at storage time, then unwrapped back to null on read.
		// containsValue(null) must return true (not NPE) — Objects.equals handles the null-vs-null comparison.
		IAdhocMap map = factory.newMapBuilder(List.of("a", "b")).append("a1").append(null).build();

		Assertions.assertThat(map.containsValue(null)).isTrue();
		// NULL_HOLDER is an internal sentinel; it is never exposed to callers
		Assertions.assertThat(map.containsValue(NullMatcher.NULL_HOLDER)).isFalse();
	}

	@Test
	public void testAppendTooMuch() {
		Assertions.assertThatThrownBy(() -> factory.newMapBuilder(List.of("a", "b")).append("a1", "b1", "c1"))
				.isInstanceOf(IllegalStateException.class);
	}

	@Test
	public void testAsyncFreezer() {
		ColumnSliceFactory sliceFactory =
				ColumnSliceFactory.builder().options(() -> Set.of(StandardQueryOptions.CONCURRENT)).build();
		MapBuilderPreKeys mapBuilder = (MapBuilderPreKeys) sliceFactory.newMapBuilder();
		Assertions.assertThat(AppendableTableUnsafe.getStrategy(((AAppendableTable) mapBuilder.pageFactory)))
				.isInstanceOf(AsynchronousFreezingStrategy.class);
	}
}
