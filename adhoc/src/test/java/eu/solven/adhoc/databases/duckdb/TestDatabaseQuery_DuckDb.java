package eu.solven.adhoc.databases.duckdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.api.v1.pojo.ColumnFilter;
import eu.solven.adhoc.database.JooqSqlDatabase;
import eu.solven.adhoc.query.DatabaseQuery;
import eu.solven.adhoc.query.GroupByColumns;

public class TestDatabaseQuery_DuckDb implements IAdhocTestConstants {
	String tableName = "someTableName";

	private Connection makeFreshInMemoryDb() {
		try {
			return DriverManager.getConnection("jdbc:duckdb:");
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	Connection dbConn = makeFreshInMemoryDb();
	JooqSqlDatabase jooqDb = new JooqSqlDatabase(() -> dbConn, tableName);

	DatabaseQuery qK1 = DatabaseQuery.builder().aggregators(Set.of(k1Sum)).build();
	DSLContext dsl = jooqDb.makeDsl();

	@Test
	public void testTableDoesNotExists() throws SQLException {
		Assertions.assertThatThrownBy(() -> jooqDb.openDbStream(qK1).collect(Collectors.toList()))
				.isInstanceOf(DataAccessException.class);
	}

	@Test
	public void testEmptyDb() throws SQLException {
		dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.VARCHAR).execute();

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(qK1).collect(Collectors.toList());

		Assertions.assertThat(dbStream).isEmpty();
	}

	@Test
	public void testReturnAll() throws SQLException {
		dsl.createTableIfNotExists(tableName).column("k1", SQLDataType.VARCHAR).execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1")).values("someKey").execute();

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(qK1).collect(Collectors.toList());

		Assertions.assertThat(dbStream).hasSize(1).contains(Map.of("k1", "someKey"));
	}

	@Test
	public void testReturn_nullableMeasures() throws SQLException {
		dsl.createTableIfNotExists(tableName)
				.column("k1", SQLDataType.VARCHAR)
				.column("k2", SQLDataType.VARCHAR)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1")).values("v1").execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k2")).values("v2").execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("k1"), DSL.field("k2")).values("v3", "v4").execute();

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(qK1).collect(Collectors.toList());

		Assertions.assertThat(dbStream).hasSize(2).contains(Map.of("k1", "v1"), Map.of("k1", "v3"));
	}

	@Test
	public void testReturn_nullableColumn_filterEquals() throws SQLException {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("b", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.VARCHAR)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", "v1").execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("b"), DSL.field("k1")).values("b1", "v2").execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("b"), DSL.field("k1"))
				.values("a2", "b2", "v3")
				.execute();

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(
				DatabaseQuery.edit(qK1).filter(ColumnFilter.builder().column("a").filtered("a1").build()).build())
				.collect(Collectors.toList());

		Assertions.assertThat(dbStream).hasSize(1).contains(Map.of("k1", "v1"));
	}

	@Test
	public void testReturn_nullableColumn_filterIn() throws SQLException {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("b", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.VARCHAR)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", "v1").execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("b"), DSL.field("k1")).values("b1", "v2").execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("b"), DSL.field("k1"))
				.values("a2", "b2", "v3")
				.execute();

		List<Map<String, ?>> dbStream = jooqDb.openDbStream(DatabaseQuery.edit(qK1)
				.filter(ColumnFilter.builder().column("a").filtered(Set.of("a1", "a2")).build())
				.explain(true)
				.build()).collect(Collectors.toList());

		Assertions.assertThat(dbStream).hasSize(2).contains(Map.of("k1", "v1"), Map.of("k1", "v3"));
	}

	@Test
	public void testReturn_nullableColumn_groupBy() throws SQLException {
		dsl.createTableIfNotExists(tableName)
				.column("a", SQLDataType.VARCHAR)
				.column("b", SQLDataType.VARCHAR)
				.column("k1", SQLDataType.VARCHAR)
				.execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("k1")).values("a1", "v1").execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("b"), DSL.field("k1")).values("b1", "v2").execute();
		dsl.insertInto(DSL.table(tableName), DSL.field("a"), DSL.field("b"), DSL.field("k1"))
				.values("a2", "b2", "v3")
				.execute();

		List<Map<String, ?>> dbStream =
				jooqDb.openDbStream(DatabaseQuery.edit(qK1).groupBy(GroupByColumns.of("a")).build())
						.collect(Collectors.toList());

		Assertions.assertThat(dbStream)
				.hasSize(3)
				.anySatisfy(m -> Assertions.assertThat(m).isEqualTo(Map.of("a", "a1", "k1", "v1")))
				.anySatisfy(m -> Assertions.assertThat((Map) m)
						.hasSize(2)
						// TODO We need an option to handle null with a default value
						.containsEntry("a", null)
						.containsEntry("k1", "v2"))
				.anySatisfy(m -> Assertions.assertThat(m).isEqualTo(Map.of("a", "a2", "k1", "v3")));
	}

}
