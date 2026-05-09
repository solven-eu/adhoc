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
package eu.solven.adhoc.table;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import eu.solven.adhoc.dataframe.row.ITabularRecordStream;
import eu.solven.adhoc.query.table.TableQueryV4;

public class TestEmptyTableWrapper {

	@Test
	public void testGetName() {
		EmptyTableWrapper table = EmptyTableWrapper.builder().name("emptyT").build();

		Assertions.assertThat(table.getName()).isEqualTo("emptyT");
	}

	@Test
	public void testGetColumns_isEmpty() {
		EmptyTableWrapper table = EmptyTableWrapper.builder().name("emptyT").build();

		Assertions.assertThat(table.getColumns()).isEmpty();
	}

	@Test
	public void testStreamSlices_emitsNothing() {
		EmptyTableWrapper table = EmptyTableWrapper.builder().name("emptyT").build();

		IQueryPod queryPod = Mockito.mock(IQueryPod.class);
		TableQueryV4 query = TableQueryV4.builder().build();

		ITabularRecordStream stream = table.streamSlices(queryPod, query);

		Assertions.assertThat(stream.toList()).isEmpty();
	}

}
