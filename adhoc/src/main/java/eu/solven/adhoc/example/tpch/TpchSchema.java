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
package eu.solven.adhoc.example.tpch;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.google.common.collect.ImmutableList;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import eu.solven.adhoc.beta.schema.AdhocSchema;
import eu.solven.adhoc.column.ColumnsManager;
import eu.solven.adhoc.cube.CubeWrapper.CubeWrapperBuilder;
import eu.solven.adhoc.measure.combination.ConstantCombination;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.forest.MeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.sum.AvgAggregation;
import eu.solven.adhoc.measure.sum.ExpressionAggregation;
import eu.solven.adhoc.query.groupby.GroupByColumns;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.sql.IDSLSupplier;
import eu.solven.adhoc.table.sql.JooqSnowflakeSchemaBuilder;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;
import eu.solven.adhoc.table.transcoder.MapTableAliaser;
import lombok.extern.slf4j.Slf4j;

/**
 * Holds the schema for the TPC-H benchmark example.
 *
 * <p>
 * TPC-H schema reference: https://docs.snowflake.com/en/user-guide/sample-data-tpch
 *
 * <p>
 * The central fact table is {@code lineitem}. Dimensions are reached via LEFT JOINs through the following snowflake
 * path:
 *
 * <pre>
 * lineitem → orders → customer → cust_nation → cust_region
 *          → part
 *          → supplier
 * </pre>
 *
 * @author Benoit Lacelle
 */
@Slf4j
@SuppressWarnings({ "PMD.AvoidDuplicateLiterals", "checkstyle:MagicNumber" })
public class TpchSchema {

	public String getName() {
		return "Tpch";
	}

	public IMeasureForest getForest(String forestName) {
		List<IMeasure> measures = new ArrayList<>();

		// Row count across all line items
		measures.add(Aggregator.countAsterisk());

		// Total quantity ordered
		measures.add(Aggregator.builder().name("quantity").columnName("l_quantity").build());

		// Revenue: sum of extended price after discount — the key TPC-H metric (cf. TPC-H Q1)
		measures.add(Aggregator.builder()
				.name("revenue")
				.aggregationKey(ExpressionAggregation.KEY)
				.columnName("sum(l_extendedprice * (1 - l_discount))")
				.build());

		// Average discount rate applied to line items
		measures.add(Aggregator.builder()
				.name("avg_discount")
				.aggregationKey(AvgAggregation.KEY)
				.columnName("l_discount")
				.build());

		// Number of distinct orders (one per unique o_orderkey)
		measures.add(Partitionor.builder()
				.name("order_count")
				.groupBy(GroupByColumns.named("o_orderkey"))
				.combinationKey(ConstantCombination.class.getName())
				.combinationOption(ConstantCombination.K_CONSTANT, 1)
				.underlying(Aggregator.countAsterisk().getName())
				.build());

		// Number of distinct customers (one per unique c_custkey)
		measures.add(Partitionor.builder()
				.name("customer_count")
				.groupBy(GroupByColumns.named("c_custkey"))
				.combinationKey(ConstantCombination.class.getName())
				.combinationOption(ConstantCombination.K_CONSTANT, 1)
				.underlying(Aggregator.countAsterisk().getName())
				.build());

		return MeasureForest.fromMeasures(forestName, measures);
	}

	public ITableWrapper getTable(String tableName) {
		IDSLSupplier dslSupplier = DuckDBHelper.inMemoryDSLSupplier();

		DSLContext dslContext = dslSupplier.getDSLContext();

		dslContext.connection(this::loadTpchData);

		JooqSnowflakeSchemaBuilder tpch = snowflakeBuilder();

		JooqTableWrapperParameters tableParameters =
				JooqTableWrapperParameters.builder().table(tpch.getSnowflakeTable()).dslSupplier(dslSupplier).build();

		return new JooqTableWrapper(tableName, tableParameters);
	}

	// `SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE` as DDL statements cannot use PreparedStatement.
	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	protected void loadTpchData(Connection connection) throws SQLException {
		try (Statement s = connection.createStatement()) {
			// TPCH is not pre-installed on all distributions
			s.execute("INSTALL tpch");
			// https://duckdb.org/docs/stable/core_extensions/tpch
			s.execute("LOAD tpch");
			// Scale factor 0.1 produces ~600 K lineitem rows; keep small for test speed
			s.execute("CALL dbgen(sf = 0.1)");
		}
	}

	protected JooqSnowflakeSchemaBuilder snowflakeBuilder() {
		// TPC-H snowflake schema — see https://docs.snowflake.com/en/user-guide/sample-data-tpch
		// lineitem is the central fact table; all other tables are reached via LEFT JOINs so that every
		// line item is preserved even when dimension rows are missing.
		return JooqSnowflakeSchemaBuilder.builder()
				.baseTable(DSL.table("lineitem"))
				.baseTableAlias("lineitem")
				.build()
				// lineitem → orders (many line items per order)
				.leftJoin(DSL.table("orders"), "orders", ImmutableList.of(Map.entry("l_orderkey", "o_orderkey")))
				// orders → customer (each order belongs to one customer)
				.leftJoin("orders",
						DSL.table("customer"),
						"customer",
						ImmutableList.of(Map.entry("o_custkey", "c_custkey")))
				// customer → nation (customer's home nation)
				.leftJoin("customer",
						DSL.table("nation"),
						"cust_nation",
						ImmutableList.of(Map.entry("c_nationkey", "n_nationkey")))
				// nation → region (customer's home region)
				.leftJoin("cust_nation",
						DSL.table("region"),
						"cust_region",
						ImmutableList.of(Map.entry("n_regionkey", "r_regionkey")))
				// lineitem → part (the ordered part)
				.leftJoin("lineitem", DSL.table("part"), "part", ImmutableList.of(Map.entry("l_partkey", "p_partkey")))
				// lineitem → supplier (the fulfilling supplier)
				.leftJoin("lineitem",
						DSL.table("supplier"),
						"supplier",
						ImmutableList.of(Map.entry("l_suppkey", "s_suppkey")));
	}

	public CubeWrapperBuilder makeCube(AdhocSchema schema,
			TpchSchema tpchSchema,
			ITableWrapper table,
			IMeasureForest forest) {
		return schema.openCubeWrapperBuilder()
				.name(tpchSchema.getName())
				.forest(forest)
				.table(table)
				.columnsManager(ColumnsManager.builder()
						.aliaser(MapTableAliaser.builder()
								.aliasToOriginals(snowflakeBuilder().getAliasToOriginal())
								.build())
						.build());
	}
}
