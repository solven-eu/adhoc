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
package eu.solven.adhoc.measure.decomposition;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.query.MeasurelessQuery;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestDecompositionHelpers {

	// ── suppressColumn (single) ─────────────────────────────────────────────

	@Test
	public void testSuppressColumn_removesFromGroupBy() {
		MeasurelessQuery step = MeasurelessQuery.builder()
				.filter(ISliceFilter.MATCH_ALL)
				.groupBy(GroupByColumns.named("color", "country"))
				.build();

		MeasurelessQuery result = DecompositionHelpers.suppressColumn(step, "color");

		Assertions.assertThat(result.getGroupBy().getGroupedByColumns()).containsExactly("country");
	}

	@Test
	public void testSuppressColumn_removesFromFilter() {
		MeasurelessQuery step = MeasurelessQuery.builder()
				.filter(ColumnFilter.matchEq("color", "pink"))
				.groupBy(IGroupBy.GRAND_TOTAL)
				.build();

		MeasurelessQuery result = DecompositionHelpers.suppressColumn(step, "color");

		Assertions.assertThat(result.getFilter()).isSameAs(ISliceFilter.MATCH_ALL);
	}

	@Test
	public void testSuppressColumn_columnNotPresent_noOp() {
		MeasurelessQuery step = MeasurelessQuery.builder()
				.filter(ISliceFilter.MATCH_ALL)
				.groupBy(GroupByColumns.named("country"))
				.build();

		MeasurelessQuery result = DecompositionHelpers.suppressColumn(step, "color");

		Assertions.assertThat(result.getGroupBy().getGroupedByColumns()).containsExactly("country");
		Assertions.assertThat(result.getFilter()).isSameAs(ISliceFilter.MATCH_ALL);
	}

	// ── suppressColumn (set) ───────────────────────────────────────────────

	@Test
	public void testSuppressColumns_multipleColumns() {
		MeasurelessQuery step = MeasurelessQuery.builder()
				.filter(ISliceFilter.MATCH_ALL)
				.groupBy(GroupByColumns.named("color", "country", "year"))
				.build();

		MeasurelessQuery result = DecompositionHelpers.suppressColumn(step, Set.of("color", "country"));

		Assertions.assertThat(result.getGroupBy().getGroupedByColumns()).containsExactly("year");
	}

	@Test
	public void testOnMissingColumn_returnsTrue() {
		// The predicate accepts missing columns (does not reject the group)
		Assertions.assertThat(DecompositionHelpers.onMissingColumn().test(null)).isTrue();
	}
}
