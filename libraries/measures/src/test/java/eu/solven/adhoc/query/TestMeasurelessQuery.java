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
package eu.solven.adhoc.query;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestMeasurelessQuery {

	@Test
	public void testBuild_minimal() {
		MeasurelessQuery query =
				MeasurelessQuery.builder().filter(ISliceFilter.MATCH_ALL).groupBy(IGroupBy.GRAND_TOTAL).build();

		Assertions.assertThat(query.getFilter()).isSameAs(ISliceFilter.MATCH_ALL);
		Assertions.assertThat(query.getGroupBy()).isSameAs(IGroupBy.GRAND_TOTAL);
		Assertions.assertThat(query.getCustomMarker()).isNull();
		Assertions.assertThat(query.getOptions()).isEmpty();
	}

	@Test
	public void testEdit_fromMeasurelessQuery() {
		MeasurelessQuery original = MeasurelessQuery.builder()
				.filter(ISliceFilter.MATCH_ALL)
				.groupBy(GroupByColumns.named("country"))
				.customMarker("marker")
				.build();

		MeasurelessQuery copy = MeasurelessQuery.edit(original).build();

		Assertions.assertThat(copy.getFilter()).isSameAs(original.getFilter());
		Assertions.assertThat(copy.getGroupBy()).isEqualTo(original.getGroupBy());
		Assertions.assertThat(copy.getCustomMarker()).isEqualTo("marker");
	}

	@Test
	public void testEdit_fromPlainWhereGroupBy() {
		// A plain IWhereGroupByQuery without IHasCustomMarker/IHasQueryOptions
		MeasurelessQuery plain = MeasurelessQuery.builder()
				.filter(ISliceFilter.MATCH_NONE)
				.groupBy(GroupByColumns.named("city"))
				.build();

		MeasurelessQuery edited = MeasurelessQuery.edit(plain).build();

		Assertions.assertThat(edited.getFilter()).isSameAs(ISliceFilter.MATCH_NONE);
		Assertions.assertThat(edited.getGroupBy().getGroupedByColumns()).containsExactly("city");
	}
}
