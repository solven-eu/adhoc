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
package eu.solven.adhoc.query.groupby;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.cube.IGroupBy;

public class TestGroupByHelpers {

	// ── union ────────────────────────────────────────────────────────────────

	@Test
	public void testUnion_disjoint() {
		IGroupBy left = GroupByColumns.named("a");
		IGroupBy right = GroupByColumns.named("b");

		IGroupBy union = GroupByHelpers.union(left, right);

		Assertions.assertThat(union.getGroupedByColumns()).containsExactlyInAnyOrder("a", "b");
	}

	@Test
	public void testUnion_overlap() {
		IGroupBy left = GroupByColumns.named("a", "b");
		IGroupBy right = GroupByColumns.named("b", "c");

		IGroupBy union = GroupByHelpers.union(left, right);

		Assertions.assertThat(union.getGroupedByColumns()).containsExactlyInAnyOrder("a", "b", "c");
	}

	@Test
	public void testUnion_grandTotalLeft() {
		IGroupBy union = GroupByHelpers.union(IGroupBy.GRAND_TOTAL, GroupByColumns.named("x"));

		Assertions.assertThat(union.getGroupedByColumns()).containsExactly("x");
	}

	@Test
	public void testUnion_bothGrandTotal() {
		IGroupBy union = GroupByHelpers.union(IGroupBy.GRAND_TOTAL, IGroupBy.GRAND_TOTAL);

		Assertions.assertThat(union.getGroupedByColumns()).isEmpty();
	}

	// ── suppressColumns ──────────────────────────────────────────────────────

	@Test
	public void testSuppressColumns_removesColumn() {
		IGroupBy groupBy = GroupByColumns.named("a", "b", "c");

		IGroupBy result = GroupByHelpers.suppressColumns(groupBy, Set.of("b"));

		Assertions.assertThat(result.getGroupedByColumns()).containsExactlyInAnyOrder("a", "c");
	}

	@Test
	public void testSuppressColumns_unknownColumn_noOp() {
		IGroupBy groupBy = GroupByColumns.named("a", "b");

		IGroupBy result = GroupByHelpers.suppressColumns(groupBy, Set.of("z"));

		Assertions.assertThat(result.getGroupedByColumns()).containsExactlyInAnyOrder("a", "b");
	}

	@Test
	public void testSuppressColumns_allColumns_grandTotal() {
		IGroupBy groupBy = GroupByColumns.named("a");

		IGroupBy result = GroupByHelpers.suppressColumns(groupBy, Set.of("a"));

		Assertions.assertThat(result.getGroupedByColumns()).isEmpty();
	}
}
