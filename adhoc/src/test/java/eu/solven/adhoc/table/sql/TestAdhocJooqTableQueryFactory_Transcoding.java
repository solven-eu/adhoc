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
package eu.solven.adhoc.table.sql;

import org.assertj.core.api.Assertions;
import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.transcoder.MapTableTranscoder;
import eu.solven.adhoc.table.transcoder.TranscodingContext;

public class TestAdhocJooqTableQueryFactory_Transcoding {
	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	AdhocJooqTableQueryFactory streamOpener = new AdhocJooqTableQueryFactory(
			MapTableTranscoder.builder().queriedToUnderlying("k1", "k").queriedToUnderlying("k2", "k").build(),
			DSL.table(DSL.name("someTableName")),
			DSL.using(SQLDialect.DUCKDB));

	TranscodingContext transcodingContext = TranscodingContext.builder().transcoder(streamOpener.transcoder).build();

	@Test
	public void testToCondition_transcodingLeadsToMatchNone() {
		Condition condition =
				streamOpener.toCondition(transcodingContext, AndFilter.and(ImmutableMap.of("k1", "v1", "k2", "v2")));

		Assertions.assertThat(condition.toString()).isEqualTo("""
				(
				  "k" = 'v1'
				  and "k" = 'v2'
				)
								""".trim());
	}

	@Test
	public void testToTableQuery_transcodingLeadsToMatchNone() {
		// BEWARE We expect a WARN. It should be turned into an Event at some point
		ResultQuery<Record> condition = streamOpener.prepareQuery(
				TableQuery.builder().filter(AndFilter.and(ImmutableMap.of("k1", "v1", "k2", "v2"))).build());

		Assertions.assertThat(condition.toString()).isEqualTo("""
				select *
				from "someTableName"
				where (
				  "k" = 'v1'
				  and "k" = 'v2'
				)
								""".trim());
	}
}
