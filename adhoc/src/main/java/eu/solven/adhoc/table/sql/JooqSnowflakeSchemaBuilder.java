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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jooq.Condition;
import org.jooq.Name;
import org.jooq.Parser;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * This class helps building a star/snowflake schema, around some baseStore and various LEFT JOINs to provide additional
 * columns.
 * <p>
 * This also facilitate the registration of proper transcoder. This implementation give priority to the first encounter
 * of a field, giving priority to the LEFT table over the RIGHT table.
 * 
 * @author Benoit Lacelle
 */
public class JooqSnowflakeSchemaBuilder {
	// JOINs often refers the same name from the joined tables: this map will record the qualified field to refer when
	// the unqualified field is queried
	// e.g. if `FROM table t JOIN joined j ON t.k = j.k`, then may decide that `k` always refers to `t.k`
	@NonNull
	// @Builder.Default
	// BEWARE Naming should follow MapTableAliaser
	@Getter
	final Map<String, String> aliasToOriginal = new ConcurrentHashMap<>();

	@NonNull
	final Table<Record> baseTable;
	@NonNull
	final String baseTableAlias;

	@Getter
	Table<Record> snowflakeTable;

	// @NonNull
	// @Default
	Supplier<Parser> parserSupplier = DSL.using(SQLDialect.DUCKDB)::parser;

	// https://stackoverflow.com/questions/30717640/how-to-exclude-property-from-lombok-builder
	// `snowflakeTable` is not built by the builder
	@Builder
	public JooqSnowflakeSchemaBuilder(Table<Record> baseTable, String baseTableAlias) {
		// this.queriedToUnderlying=queriedToUnderlying;
		this.baseTable = baseTable;
		this.baseTableAlias = baseTableAlias;

		this.snowflakeTable = baseTable.as(baseTableAlias);
	}

	/**
	 * Assumes the LEFT table is the base store.
	 *
	 * @param joinedTable
	 *            the JOINed table
	 * @param joinName
	 *            the alias of the JOINed table
	 * @param on
	 *            a {@link List} of mapping of `.isEqualTo` between LEFT fields and RIGHT fields
	 * @return
	 */
	public JooqSnowflakeSchemaBuilder leftJoin(Table<?> joinedTable,
			String joinName,
			List<Map.Entry<String, String>> on) {
		return leftJoin(baseTableAlias, joinedTable, joinName, on);
	}

	// TODO Should this rely on `AdhocJooqHelper.name(name, dslContext::parser);`?
	// Coding it manually looks bad. But this syntax may not depends on the parser (e.g. DuckDB vs another syntax)
	protected Name parseOnName(String leftTableAlias, String rawName) {
		if (rawName.startsWith(";")) {
			return DSL.unquotedName(rawName.substring(1));
		} else {
			Name name = AdhocJooqHelper.name(rawName, parserSupplier);

			if (name.parts().length == 1) {
				name = DSL.name(DSL.name(leftTableAlias), name);
			}

			return name;
		}
	}

	/**
	 * @param leftTableAlias
	 *            the name of the LEFT/base table. May not be the root/base table given we manage snowflake schema.
	 * @param joinedTable
	 *            the JOINed table
	 * @param joinName
	 *            the alias of the JOINed table
	 * @param on
	 *            a {@link List} of mapping of `.isEqualTo` between LEFT fields and RIGHT fields
	 * @return
	 */
	public JooqSnowflakeSchemaBuilder leftJoin(String leftTableAlias,
			Table<?> joinedTable,
			String joinName,
			List<Map.Entry<String, String>> on) {
		List<Condition> onConditions = on.stream().map(e -> {
			Name leftName = parseOnName(leftTableAlias, e.getKey());
			Name rightName = parseOnName(joinName, e.getValue());

			registerInTranscoder(leftName, rightName);

			return DSL.field(leftName).eq(DSL.field(rightName));
		}).toList();

		return leftJoinConditions(joinedTable.as(joinName), onConditions);
	}

	public JooqSnowflakeSchemaBuilder joinHomo(String leftTableAlias,
			Table<?> joinedTable,
			String joinName,
			List<String> on) {
		return leftJoin(leftTableAlias, joinedTable, joinName, on.stream().map(f -> Map.entry(f, f)).toList());
	}

	protected void registerInTranscoder(Name leftName, Name rightName) {
		// `putIfAbsent`: priority to the first occurrence of the field
		// `leftTableAlias`; priority to the LEFT/base table than the RIGHT/joined table
		String queryName = leftName.last();
		String tableName = leftName.toString();

		if (!Objects.equals(queryName, tableName)) {
			aliasToOriginal.putIfAbsent(queryName, tableName);
		}
	}

	public JooqSnowflakeSchemaBuilder leftJoinConditions(Table<?> joinedTable, List<Condition> on) {
		snowflakeTable = snowflakeTable.leftJoin(joinedTable).on(on.toArray(Condition[]::new));

		return this;
	}

	/**
	 *
	 * @param builderConsumer
	 *            enable chaining definitions without inlining
	 * @return this
	 */
	public JooqSnowflakeSchemaBuilder accept(Consumer<JooqSnowflakeSchemaBuilder> builderConsumer) {
		builderConsumer.accept(this);

		return this;
	}

	/**
	 *
	 * @param builderConsumers
	 *            enable chaining definitions without inlining
	 * @return this
	 */
	public JooqSnowflakeSchemaBuilder accept(Iterable<Consumer<JooqSnowflakeSchemaBuilder>> builderConsumers) {
		builderConsumers.forEach(builderConsumer -> builderConsumer.accept(this));

		return this;
	}

	/**
	 * Lombok @Builder
	 * 
	 * @author Benoit Lacelle
	 */
	// public static class JooqSnowflakeSchemaBuilderBuilder {
	//
	// // https://github.com/projectlombok/lombok/issues/2307#issuecomment-1119511303
	// private JooqSnowflakeSchemaBuilderBuilder snowflakeTable(Table<Record> snowflakeTable) {
	// return this;
	// }
	// }

}
