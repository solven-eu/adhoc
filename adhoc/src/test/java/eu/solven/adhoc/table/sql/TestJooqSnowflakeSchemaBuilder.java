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

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

public class TestJooqSnowflakeSchemaBuilder {
	static {
		// https://stackoverflow.com/questions/28272284/how-to-disable-jooqs-self-ad-message-in-3-4
		System.setProperty("org.jooq.no-logo", "true");
		// https://stackoverflow.com/questions/71461168/disable-jooq-tip-of-the-day
		System.setProperty("org.jooq.no-tips", "true");
	}

	@Test
	public void testSnowflake() {
		JooqSnowflakeSchemaBuilder snowflakeBuilder =
				JooqSnowflakeSchemaBuilder.builder().baseTable(DSL.table("baseTable")).baseTableAlias("base").build();

		snowflakeBuilder.leftJoin(DSL.table("joinedTable"), "joined", List.of(Map.entry("baseA", "joinedA")));

		Table<Record> snowflakeTable = snowflakeBuilder.getSnowflakeTable();

		Assertions.assertThat(snowflakeTable.toString()).isEqualTo("""
				"base"
				  left outer join "joined"
				    on "base"."baseA" = "joined"."joinedA"
				""".trim());

		Assertions.assertThat(snowflakeBuilder.getQueriedToUnderlying())
				.hasSize(1)
				.containsEntry("baseA", "\"base\".\"baseA\"");
	}

	@Test
	public void testSnowflake_fieldWithDot() {
		JooqSnowflakeSchemaBuilder snowflakeBuilder =
				JooqSnowflakeSchemaBuilder.builder().baseTable(DSL.table("baseTable")).baseTableAlias("base").build();

		snowflakeBuilder.leftJoin(DSL.table("joinedTable1"), "joined1", List.of(Map.entry("baseA", "joined1A")));
		snowflakeBuilder
				.leftJoin(DSL.table("joinedTable2"), "joined2", List.of(Map.entry("joined1.baseA", "joined2A")));

		Table<Record> snowflakeTable = snowflakeBuilder.getSnowflakeTable();

		Assertions.assertThat(snowflakeTable.toString()).isEqualTo("""
				"base"
				  left outer join "joined1"
				    on "base"."baseA" = "joined1"."joined1A"
				  left outer join "joined2"
				    on "joined1"."baseA" = "joined2"."joined2A"
				                """.trim());

		Assertions.assertThat(snowflakeBuilder.getQueriedToUnderlying())
				.hasSize(2)
				.containsEntry("baseA", "\"base\".\"baseA\"")
				// TODO This second entry is faulty
				.containsEntry("joined1.baseA", "\"base\".\"joined1.baseA\"");
	}
}
