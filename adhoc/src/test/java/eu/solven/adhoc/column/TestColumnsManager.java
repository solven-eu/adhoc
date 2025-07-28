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
package eu.solven.adhoc.column;

import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.table.transcoder.ITableReverseTranscoder;
import eu.solven.adhoc.table.transcoder.ITableTranscoder;
import eu.solven.adhoc.table.transcoder.MapTableTranscoder;
import eu.solven.adhoc.table.transcoder.TranscodingContext;

public class TestColumnsManager {
	@Test
	public void testTranscodeExpressionColumn() {
		ITableTranscoder transcoder = MapTableTranscoder.builder().queriedToUnderlying("queried", "underlying").build();
		ColumnsManager columnsManager = ColumnsManager.builder().transcoder(transcoder).build();

		TranscodingContext context = columnsManager.openTranscodingContext();

		IAdhocGroupBy groupBy =
				GroupByColumns.of(TableExpressionColumn.builder().name("underlying").sql("someSql").build());
		IAdhocGroupBy transcoded = columnsManager.transcodeGroupBy(context, groupBy);

		Assertions.assertThat(transcoded)
				.isEqualTo(
						GroupByColumns.of(TableExpressionColumn.builder().name("underlying").sql("someSql").build()));

		ITableReverseTranscoder reversed = columnsManager.prepareColumnTranscoder(context);

		Assertions.assertThat(reversed.estimateQueriedSize(Set.of("underlying"))).isEqualTo(1);
		Assertions.assertThat(reversed.queried("underlying")).containsExactly("underlying");
	}
}
