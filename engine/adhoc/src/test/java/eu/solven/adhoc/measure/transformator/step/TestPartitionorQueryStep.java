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
package eu.solven.adhoc.measure.transformator.step;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestPartitionorQueryStep {

	private PartitionorQueryStep makeStep(IGroupBy stepGroupBy, IGroupBy partitionorGroupBy, String... underlyings) {
		Partitionor partitionor =
				Partitionor.builder().name("p").underlyings(List.of(underlyings)).groupBy(partitionorGroupBy).build();

		CubeQueryStep step = CubeQueryStep.builder().measure("p").groupBy(stepGroupBy).build();

		return new PartitionorQueryStep(partitionor, Mockito.mock(IAdhocFactories.class), step);
	}

	// union(step={a}, partitionor={b}) = {a, b}: step column a is a prefix → no break.
	@Test
	public void isBreakSorting_oneUnderlying_partitionorAddsCompatibleColumn() {
		PartitionorQueryStep step = makeStep(GroupByColumns.named("a"), GroupByColumns.named("b"), "u1");

		Assertions.assertThat(step.getUnderlyingGroupBy().getSequencedColumns()).containsExactly("a", "b");
		Assertions.assertThat(step.isBreakSorting()).isFalse();
	}

	// Two underlyings, both get the same groupBy from union → same result as one underlying.
	@Test
	public void isBreakSorting_twoUnderlyings_equivalentGroupBy() {
		PartitionorQueryStep step = makeStep(GroupByColumns.named("a"), GroupByColumns.named("b"), "u1", "u2");

		Assertions.assertThat(step.getUnderlyingGroupBy().getSequencedColumns()).containsExactly("a", "b");
		Assertions.assertThat(step.isBreakSorting()).isFalse();
	}

	// union puts step columns first, so step columns are always a prefix of the underlying groupBy.
	// isBreakSorting is therefore false even when the partitionor column comes before the step column alphabetically.
	@Test
	public void isBreakSorting_oneUnderlying_partitionorAddsColumnBeforeAlphabetically() {
		// step={b}, partitionor={a} → union={b,a} (b first) → breakSorting({b,a},{b}) = false
		PartitionorQueryStep step = makeStep(GroupByColumns.named("b"), GroupByColumns.named("a"), "u1");

		Assertions.assertThat(step.getUnderlyingGroupBy().getSequencedColumns()).containsExactly("b", "a");
		Assertions.assertThat(step.isBreakSorting()).isFalse();
	}

	@Test
	public void isBreakSorting_partitionorReverseStep() {
		PartitionorQueryStep step = makeStep(GroupByColumns.named("b", "a"), GroupByColumns.named("a", "b"), "u1");

		Assertions.assertThat(step.getUnderlyingGroupBy().getSequencedColumns()).containsExactly("b", "a");
		Assertions.assertThat(step.isBreakSorting()).isFalse();
	}

	@Test
	public void isBreakSorting_intersection() {
		PartitionorQueryStep step = makeStep(GroupByColumns.named("a", "b"), GroupByColumns.named("b", "c"), "u1");

		Assertions.assertThat(step.getUnderlyingGroupBy().getSequencedColumns()).containsExactly("a", "b", "c");
		Assertions.assertThat(step.isBreakSorting()).isFalse();
	}

}
