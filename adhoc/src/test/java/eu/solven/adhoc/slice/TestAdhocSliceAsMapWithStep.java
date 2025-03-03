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
package eu.solven.adhoc.slice;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.groupby.GroupByColumns;

public class TestAdhocSliceAsMapWithStep implements IAdhocTestConstants {
	@Test
	public void testAsFilter() {
		IAdhocFilter stepFilter = ColumnFilter.isEqualTo("c1", "v1");
		AdhocQueryStep step =
				AdhocQueryStep.builder().measure(k1Sum).filter(stepFilter).groupBy(GroupByColumns.named("c2")).build();
		IAdhocSlice parentSlice = SliceAsMap.fromMap(Map.of("c2", "v2"));

		SliceAsMapWithStep slice = SliceAsMapWithStep.builder().queryStep(step).slice(parentSlice).build();

		Assertions.assertThat(slice.asFilter()).isEqualTo(AndFilter.and(Map.of("c1", "v1", "c2", "v2")));
	}
}
