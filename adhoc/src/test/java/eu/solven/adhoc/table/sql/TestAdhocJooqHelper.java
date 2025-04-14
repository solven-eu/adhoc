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

public class TestAdhocJooqHelper {
	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	@Test
	public void testIsExpression() {
		Assertions.assertThat(AdhocJooqHelper.isExpression("foo")).isFalse();
		Assertions.assertThat(AdhocJooqHelper.isExpression("*")).isTrue();
	}

	@Test
	public void testName() {
		Assertions.assertThat(AdhocJooqHelper.name("foo", DSL.using(SQLDialect.DUCKDB)::parser).toString())
				.isEqualTo("\"foo\"");
		Assertions.assertThat(AdhocJooqHelper.name("foo.bar", DSL.using(SQLDialect.DUCKDB)::parser).toString())
				.isEqualTo("\"foo\".\"bar\"");
		Assertions.assertThat(AdhocJooqHelper.name("foo bar", DSL.using(SQLDialect.DUCKDB)::parser).toString())
				.isEqualTo("\"foo bar\"");

		// This case is not very legitimate from standard SQL perspective
		// Still, we parse it as if whitespace was not a special character
		Assertions.assertThat(AdhocJooqHelper.name("foo.bar zoo", DSL.using(SQLDialect.DUCKDB)::parser).toString())
				.isEqualTo("\"foo\".\"bar zoo\"");
	}
}
