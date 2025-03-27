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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

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
import eu.solven.adhoc.query.table.TableQuery;
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
		return () -> {
			Connection duplicated;
			try {
				duplicated = duckDbConnection.duplicate();
			} catch (SQLException e) {
				throw new IllegalStateException("Issue duplicating an InMemory DuckDB connection", e);
			}
			return DSL.using(duplicated);
		};
	}

	public static CoordinatesSample getCoordinates(AdhocJooqTableWrapper table,
			String column,
			IValueMatcher valueMatcher,
			int limit) {
		Name columnName = DSL.name(column.split("\\."));

		// select approx_count_distinct("id") "approx_count_distinct" from
		// read_parquet('/Users/blacelle/Downloads/datasets/adresses-france-10-2024.parquet') group by ()
		TableQuery estimatedCardinalityQuery = TableQuery.builder()
				// https://duckdb.org/docs/stable/sql/functions/aggregates.html#approximate-aggregates
				.aggregator(Aggregator.builder()
						.aggregationKey(ExpressionAggregation.KEY)
						.name("approx_count_distinct")
						.columnName("approx_count_distinct(%s)".formatted(columnName))
						.build())
				// .explain(true)
				.build();

		long estimatedCardinality;
		Optional<ITabularRecord> optCardinalityRecord = table.streamSlices(estimatedCardinalityQuery).asMap().findAny();
		if (optCardinalityRecord.isEmpty()) {
			estimatedCardinality = 0L;
		} else {
			ITabularRecord tabularRecord = optCardinalityRecord.get();

			// TODO Enable getting by index
			estimatedCardinality =
					(long) IValueProvider.getValue(vc -> tabularRecord.onAggregate("approx_count_distinct", vc));
		}

		int returnedCoordinates;

		if (limit < 1) {
			returnedCoordinates = Integer.MAX_VALUE;
		} else {
			returnedCoordinates = limit;
		}

		// select approx_top_k("id", 100) "approx_count_distinct" from
		// read_parquet('/Users/blacelle/Downloads/datasets/adresses-france-10-2024.parquet') group by ();
		TableQuery tableQuery = TableQuery.builder()
				// https://duckdb.org/docs/stable/sql/functions/aggregates.html#approximate-aggregates
				.aggregator(Aggregator.builder()
						.aggregationKey(ExpressionAggregation.KEY)
						.name("approx_top_k")
						.columnName("approx_top_k(%s, %s)".formatted(columnName, returnedCoordinates))
						.build())
				// .explain(true)
				.build();

		List<Object> coordinates;
		Optional<ITabularRecord> optTopK = table.streamSlices(tableQuery).asMap().findAny();
		if (optTopK.isEmpty()) {
			coordinates = ImmutableList.of();
		} else {
			ITabularRecord tabularRecord = optTopK.get();

			// TODO Enable getting by index
			// TODO Is it important to call `Array.free()`?
			java.sql.Array array =
					(java.sql.Array) IValueProvider.getValue(vc -> tabularRecord.onAggregate("approx_top_k", vc));

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
		}

		return CoordinatesSample.builder().coordinates(coordinates).estimatedCardinality(estimatedCardinality).build();
	}
}
