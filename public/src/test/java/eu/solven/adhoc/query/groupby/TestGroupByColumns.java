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
import com.google.common.base.MoreObjects;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.pepper.unittest.PepperJacksonTestHelper;
import lombok.Builder;
import lombok.Getter;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TestGroupByColumns {
	@Test
	public void testHashcodeEquals() {
		EqualsVerifier.forClass(GroupByColumns.class).withIgnoredFields("cachedNameToColumn").verify();
	}

	@Test
	public void testDifferentOrders() {
		IGroupBy groupByAsc = GroupByColumns.named("a", "b");
		IGroupBy groupByDesc = GroupByColumns.named("b", "a");

		Assertions.assertThat(groupByAsc).isEqualTo(groupByDesc);
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		IGroupBy groupByAsc = GroupByColumns.named("a", "b");

		String asString = PepperJacksonTestHelper.verifyJackson(IGroupBy.class, groupByAsc);
		Assertions.assertThat(asString).isEqualTo("""
				{
				  "columns" : [ "a", "b" ]
				}""");
	}

	@Test
	public void testOf_multipleSameRef() {
		IGroupBy groupBy = GroupByColumns.named("a", "b", "a");
		Assertions.assertThat(groupBy).isEqualTo(GroupByColumns.named("a", "b"));
	}

	@Test
	public void testToString() {
		Assertions.assertThat(GroupByColumns.grandTotal()).hasToString("grandTotal");

		IGroupBy groupBy = GroupByColumns.named("a", "b");
		Assertions.assertThat(groupBy).hasToString("(a, b)");
	}

	@Test
	public void testToString_refHasComa() {
		Assertions.assertThat(GroupByColumns.grandTotal()).hasToString("grandTotal");

		IGroupBy groupBy = GroupByColumns.named("a", "b,c");
		Assertions.assertThat(groupBy).hasToString("(a, \"b,c\")");
	}

	@Builder
	@Getter
	public static class CustomTestColumn implements IAdhocColumn {
		String name;

		@Override
		public String toString() {
			return MoreObjects.toStringHelper(this).add("name", name).toString();
		}
	}

	@Test
	public void testToString_customColumn() {
		IGroupBy groupBy = GroupByColumns.of(ReferencedColumn.ref("a"), CustomTestColumn.builder().name("b").build());
		Assertions.assertThat(groupBy).hasToString("(a, CustomTestColumn{name=b})");
	}
}
