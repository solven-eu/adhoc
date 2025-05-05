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
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.measure.operator.StandardOperatorsFactory;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.table.transcoder.ITableTranscoder;
import eu.solven.adhoc.table.transcoder.MapTableTranscoder;
import eu.solven.adhoc.table.transcoder.TranscodingContext;

/**
 * BEWARE This unitTests is useless since transcoding has been moved to {@link IColumnsManager}.
 * 
 * @author Benoit Lacelle
 */
public class TestJooqTableQueryFactory_Transcoding {
	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	ITableTranscoder transcoder =
			MapTableTranscoder.builder().queriedToUnderlying("k1", "k").queriedToUnderlying("k2", "k").build();
	JooqTableQueryFactory streamOpener = new JooqTableQueryFactory(new StandardOperatorsFactory(),
			DSL.table(DSL.name("someTableName")),
			DSL.using(SQLDialect.DUCKDB));

	TranscodingContext transcodingContext = TranscodingContext.builder().transcoder(transcoder).build();

	@Test
	public void testToCondition_transcodingLeadsToMatchNone() {
		JooqTableQueryFactory.ConditionWithFilter condition =
				streamOpener.toCondition(AndFilter.and(ImmutableMap.of("k1", "v1", "k2", "v2")));

		Assertions.assertThat(condition.getPostFilter()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getCondition().toString()).isEqualTo("""
				(
				  "k1" = 'v1'
				  and "k2" = 'v2'
				)""");
	}

	@Test
	public void testToTableQuery_transcodingLeadsToMatchNone() {
		// BEWARE We expect a WARN. It should be turned into an Event at some point
		IJooqTableQueryFactory.QueryWithLeftover condition = streamOpener.prepareQuery(
				TableQuery.builder().filter(AndFilter.and(ImmutableMap.of("k1", "v1", "k2", "v2"))).build());

		Assertions.assertThat(condition.getLeftover()).satisfies(l -> Assertions.assertThat(l.isMatchAll()).isTrue());
		Assertions.assertThat(condition.getQuery().toString()).isEqualTo("""
				select 1
				from "someTableName"
				where (
				  "k1" = 'v1'
				  and "k2" = 'v2'
				)
				group by ()""");
	}
}
