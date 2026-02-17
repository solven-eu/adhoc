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
package eu.solven.adhoc.cube.training.a_basics;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * A GROUP BY is a typical OLAP operation, when data are aggregated at given granularity. The granularity is defined by
 * expressing columns.
 * 
 * If no column is expressed, we're doing a grandTotal. The query will return a single slice: `Map.of()` but the cube
 * might have been filtered given a `WHERE` clause though a ISliceFilter.
 * 
 * @author Benoit Lacelle
 */
public class HelloGroupBy {
	@Test
	public void helloGroupBy() {
		IAdhocGroupBy groupBy = GroupByColumns.named("color", "ccy");

		Assertions.assertThat(groupBy.getGroupedByColumns()).containsExactly("ccy", "color");
		Assertions.assertThat(groupBy.getNameToColumn().get("color")).isEqualTo(ReferencedColumn.ref("color"));
		Assertions.assertThat(groupBy.getNameToColumn().get("ccy")).isEqualTo(ReferencedColumn.ref("ccy"));
	}
}
