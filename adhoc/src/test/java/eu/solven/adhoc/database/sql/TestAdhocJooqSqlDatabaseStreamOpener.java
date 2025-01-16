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
package eu.solven.adhoc.database.sql;

import org.assertj.core.api.Assertions;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.api.v1.pojo.AndFilter;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.api.v1.pojo.OrFilter;
import eu.solven.adhoc.database.IdentityTranscoder;

public class TestAdhocJooqSqlDatabaseStreamOpener {
	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
	}

	AdhocJooqSqlDatabaseStreamOpener streamOpener = new AdhocJooqSqlDatabaseStreamOpener(new IdentityTranscoder(),
			DSL.name("someTableName"),
			Mockito.mock(DSLContext.class));

	@Test
	public void testToCondition_ColumnEquals() {
		Condition condition = streamOpener.toCondition(ColumnFilter.isEqualTo("k1", "v1"));

		Assertions.assertThat(condition.toString()).isEqualTo("""
				"k1" = 'v1'
				""".trim());
	}

	@Test
	public void testToCondition_AndColumnsEquals() {
		// ImmutableMap for ordering, as we later check the .toString
		Condition condition =
				streamOpener.toCondition(AndFilter.andAxisEqualsFilters(ImmutableMap.of("k1", "v1", "k2", "v2")));

		Assertions.assertThat(condition.toString()).isEqualTo("""
				(
				  "k1" = 'v1'
				  and "k2" = 'v2'
				)
				""".trim());
	}

	@Test
	public void testToCondition_OrColumnsEquals() {
		Condition condition = streamOpener
				.toCondition(OrFilter.or(ColumnFilter.isEqualTo("k1", "v1"), ColumnFilter.isEqualTo("k2", "v2")));

		Assertions.assertThat(condition.toString()).isEqualTo("""
				(
				  "k1" = 'v1'
				  or "k2" = 'v2'
				)
				""".trim());
	}
}
