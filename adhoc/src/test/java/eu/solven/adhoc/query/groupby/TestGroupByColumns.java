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
package eu.solven.adhoc.query.groupby;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.solven.adhoc.column.FunctionCalculatedColumn;
import eu.solven.adhoc.column.ICalculatedColumn;
import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.pepper.unittest.PepperJacksonTestHelper;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TestGroupByColumns {
	@Test
	public void testHashcodeEquals() {
		EqualsVerifier.forClass(GroupByColumns.class).withIgnoredFields("cachedNameToColumn").verify();
	}

	@Test
	public void testDifferentOrders() {
		IAdhocGroupBy groupByAsc = GroupByColumns.named("a", "b");
		IAdhocGroupBy groupByDesc = GroupByColumns.named("b", "a");

		Assertions.assertThat(groupByAsc).isEqualTo(groupByDesc);
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		IAdhocGroupBy groupByAsc = GroupByColumns.named("a", "b");

		String asString = PepperJacksonTestHelper.verifyJackson(IAdhocGroupBy.class, groupByAsc);
		Assertions.assertThat(asString).isEqualTo("""
				{
				  "columns" : [ "a", "b" ]
				}""");
	}

	@Test
	public void testOf_multipleSameRef() {
		IAdhocGroupBy groupBy = GroupByColumns.named("a", "b", "a");
		Assertions.assertThat(groupBy).isEqualTo(GroupByColumns.named("a", "b"));
	}

	@Test
	public void testOf_multiplename_differentType() {
		ICalculatedColumn calculatedColumn =
				FunctionCalculatedColumn.builder().name("a").recordToCoordinate(r -> r.getGroupBy("a_")).build();

		Assertions
				.assertThatThrownBy(
						() -> GroupByColumns.of(ReferencedColumn.ref("a"), ReferencedColumn.ref("b"), calculatedColumn))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
