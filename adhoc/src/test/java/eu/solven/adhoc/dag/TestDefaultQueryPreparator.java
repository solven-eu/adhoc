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
package eu.solven.adhoc.dag;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.dag.context.DefaultQueryPreparator;
import eu.solven.adhoc.dag.context.ExecutingQueryContext;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.query.AdhocQueryId;
import eu.solven.adhoc.query.cube.AdhocSubQuery;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.query.cube.ICubeQuery;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.table.ITableWrapper;

public class TestDefaultQueryPreparator {
	ITableWrapper table = Mockito.mock(ITableWrapper.class);
	IMeasureForest forest = Mockito.mock(IMeasureForest.class);
	IColumnsManager columnManager = Mockito.mock(IColumnsManager.class);

	{
		Mockito.when(table.getName()).thenReturn("someTableName");
	}

	@Test
	public void testSubQuery() {
		DefaultQueryPreparator queryPreparator =
				DefaultQueryPreparator.builder().implicitFilter(f -> ColumnFilter.isEqualTo("c", "v")).build();

		ICubeQuery query = CubeQuery.builder().measure("m").build();
		AdhocSubQuery subQuery =
				AdhocSubQuery.builder().subQuery(query).parentQueryId(AdhocQueryId.from("someCube", query)).build();

		ExecutingQueryContext prepared = queryPreparator.prepareQuery(table, forest, columnManager, subQuery);

		Assertions.assertThat(prepared.getQueryId().getParentQueryId())
				.isEqualTo(subQuery.getParentQueryId().getQueryId());
	}
}
