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
package eu.solven.adhoc.table.sql.duckdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.duckdb.DuckDBConnection;
import org.jooq.Name;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.ExpressionAggregation;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQuery.TableQueryBuilder;
import eu.solven.adhoc.table.sql.AdhocJooqHelper;
import eu.solven.adhoc.table.sql.DSLSupplier;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.NonNull;

/**
 * Helps working with DuckDB.
 */
public class DuckDbHelper {
	protected DuckDbHelper() {
		// hidden
	}

	/**
	 * This {@link DuckDBConnection} should be `.duplicate` in case of multi-threaded access.
	 *
	 * Everything which not persisted is discarded when the connection is closed: it can be used to read/write files,
	 * but any inMemory tables would be dropped.
	 *
	 * @return a {@link DuckDBConnection} to an new DuckDB InMemory instance.
	 */
	// https://duckdb.org/docs/api/java.html
	public static DuckDBConnection makeFreshInMemoryDb() {
		try {
			return (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		} catch (SQLException e) {
			throw new IllegalStateException("Issue opening an InMemory DuckDB", e);
		}
	}

	/**
	 *
	 * @return a {@link DSLSupplier} based on provided {@link SQLDialect}
	 */
	public static @NonNull DSLSupplier inMemoryDSLSupplier() {
		DuckDBConnection duckDbConnection = DuckDbHelper.makeFreshInMemoryDb();
		return dslSupplier(duckDbConnection);
	}

	public static DSLSupplier dslSupplier(DuckDBConnection duckDbConnection) {
		return () -> {
			Connection duplicated;
			try {
				duplicated = duckDbConnection.duplicate();
			} catch (SQLException e) {
				throw new IllegalStateException("Issue duplicating an InMemory DuckDB connection", e);
			}
			return DSLSupplier.fromConnection(() -> duplicated).getDSLContext();
		};
	}

	public static CoordinatesSample getCoordinates(JooqTableWrapper table,
			String column,
			IValueMatcher valueMatcher,
			int limit) {
		return getCoordinates(table, Map.of(column, valueMatcher), limit).get(column);
	}

	/**
	 * Compute in a single query the estimated cardinality and a selection of the most present coordinates specialized
	 * for DuckDB.
	 * 
	 * @param table
	 * @param columnToValueMatcher
	 * @param limit
	 *            the maximum number of coordinates per column
	 * @return
	 */
	public static Map<String, CoordinatesSample> getCoordinates(JooqTableWrapper table,
			Map<String, IValueMatcher> columnToValueMatcher,
			int limit) {
		if (columnToValueMatcher.isEmpty()) {
			return Map.of();
		}

		List<String> columns = new ArrayList<>();

		// select approx_count_distinct("id") "approx_count_distinct", approx_top_k("id", 100) "approx_top_k" from
		// read_parquet('/Users/blacelle/Downloads/datasets/adresses-france-10-2024.parquet') group by ()
		TableQueryBuilder queryBuilder = TableQuery.builder();

		columnToValueMatcher.forEach((column, valueMatcher) -> {
			int columnIndex = columns.size();
			columns.add(column);
			appendGetCoordinatesMeasures(limit, column, valueMatcher, columnIndex, queryBuilder);
		});

		TableQuery tableQuery = queryBuilder.build();

		Optional<ITabularRecord> optCardinalityRecord = table.streamSlices(tableQuery).records().findAny();
		if (optCardinalityRecord.isEmpty()) {
			Map<String, CoordinatesSample> columnToCoordinates = new TreeMap<>();

			columns.forEach(column -> columnToCoordinates.put(column, CoordinatesSample.empty()));

			return columnToCoordinates;
		} else {
			Map<String, CoordinatesSample> columnToCoordinates = new TreeMap<>();
			ITabularRecord tabularRecord = optCardinalityRecord.get();

			for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
				String measuresSuffix = "_" + columnIndex;

				long estimatedCardinality = (long) IValueProvider
						.getValue(tabularRecord.onAggregate("approx_count_distinct" + measuresSuffix));

				// TODO Is it important to call `Array.free()`?
				java.sql.Array array = (java.sql.Array) IValueProvider
						.getValue(tabularRecord.onAggregate("approx_top_k" + measuresSuffix));

				List<Object> coordinates;
				if (array == null) {
					// BEWARE When does this happen?
					coordinates = ImmutableList.of();
				} else {
					try {
						Object[] nativeArray = (Object[]) array.getArray();

						if (nativeArray.length >= 1 && nativeArray[0] instanceof java.sql.Blob) {
							// BEWARE We should have skip the search altogether
							// Returning a Blob, or a `byte[]` has unclear usage/support
							coordinates = ImmutableList.of();
						} else {
							coordinates = ImmutableList.copyOf(nativeArray);
						}
					} catch (SQLException e) {
						throw new IllegalArgumentException(e);
					}
				}

				columnToCoordinates.put(columns.get(columnIndex),
						CoordinatesSample.builder()
								.coordinates(coordinates)
								.estimatedCardinality(estimatedCardinality)
								.build());
			}

			return columnToCoordinates;
		}

	}

	static void appendGetCoordinatesMeasures(int limit,
			String column,
			IValueMatcher valueMatcher,
			int columnIndex,
			TableQueryBuilder queryBuilder) {
		String measuresSuffix = "_" + columnIndex;

		Name columnName = AdhocJooqHelper.name(column, () -> DSL.using(SQLDialect.DUCKDB).parser());

		int returnedCoordinates;

		if (limit < 0) {
			returnedCoordinates = Integer.MAX_VALUE;
		} else {
			returnedCoordinates = limit;
		}
		String countExpression = "approx_count_distinct(%s)".formatted(columnName);
		String topKExpression = "approx_top_k(%s, %s)".formatted(columnName, returnedCoordinates);

		if (!IValueMatcher.MATCH_ALL.equals(valueMatcher)) {
			String filterExpression = toFilterExpression(valueMatcher);
			String filterSuffix = " FILTER (CAST(%s AS VARCHAR) %s)".formatted(columnName, filterExpression);

			countExpression += filterSuffix;
			topKExpression += filterSuffix;
		}

		queryBuilder
				// https://duckdb.org/docs/stable/sql/functions/aggregates.html#approximate-aggregates
				.aggregator(Aggregator.builder()
						.aggregationKey(ExpressionAggregation.KEY)
						.name("approx_count_distinct" + measuresSuffix)
						.columnName(countExpression)
						.build())

				// https://duckdb.org/docs/stable/sql/functions/aggregates.html#approximate-aggregates
				.aggregator(Aggregator.builder()
						.aggregationKey(ExpressionAggregation.KEY)
						.name("approx_top_k" + measuresSuffix)
						.columnName(topKExpression)
						.build());
	}

	/**
	 * 
	 * @param valueMatcher
	 * @return a SQL expression matching input IValueMatcher for a `FILTER` expression
	 */
	// https://duckdb.org/docs/stable/sql/query_syntax/filter.html
	static String toFilterExpression(IValueMatcher valueMatcher) {
		if (IValueMatcher.MATCH_ALL.equals(valueMatcher)) {
			return "1 = 1";
		} else if (IValueMatcher.MATCH_NONE.equals(valueMatcher)) {
			return "1 = 0";
		} else if (valueMatcher instanceof NotMatcher notMatcher) {
			return "NOT " + toFilterExpression(notMatcher.getNegated());
		} else if (valueMatcher instanceof LikeMatcher likeMatcher) {
			return "LIKE '%s'".formatted(likeMatcher.getLike());
		} else {
			throw new NotYetImplementedException(
					"TODO: FILTER expression for %s".formatted(PepperLogHelper.getObjectAndClass(valueMatcher)));
		}
	}
}
