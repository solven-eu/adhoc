/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.duckdb.DuckDBResultSet;
import org.jooq.ConnectionProvider;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.conf.ParamType;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordFactory;
import eu.solven.adhoc.engine.cancel.CancelledQueryException;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.exception.AdhocExceptionHelpers;
import eu.solven.adhoc.table.sql.IJooqTableQueryFactory;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * Enables querying DuckDB and fetching the result through Arrow.
 *
 * @author Benoit Lacelle
 */
// https://duckdb.org/2023/08/04/adbc
@Slf4j
public class DuckDBTableWrapper extends JooqTableWrapper {

	final DuckDBTableWrapperParameters duckDBParameters;

	@Builder(builderMethodName = "duckdb")
	public DuckDBTableWrapper(String name, DuckDBTableWrapperParameters duckDBParameters) {
		super(name, duckDBParameters.getBase());

		this.duckDBParameters = duckDBParameters;
	}

	/**
	 * Boiler-plate to close many {@link AutoCloseable}.
	 * 
	 * Resources are closed in reversed order.
	 * 
	 * @param resources
	 */
	private static void closeAll(List<AutoCloseable> resources) {
		for (int i = resources.size() - 1; i >= 0; i--) {
			try {
				resources.get(i).close();
			} catch (Exception e) {
				log.warn("Error closing resource", e);
			}
		}
	}

	@Override
	protected Stream<ITabularRecord> streamTabularRecords(QueryPod queryPod,
			IJooqTableQueryFactory.QueryWithLeftover sqlQuery,
			ITabularRecordFactory tabularRecordFactory) {
		return sqlQuery.getQueries()
				.stream()
				.flatMap(oneQuery -> toArrowStream(queryPod, oneQuery, tabularRecordFactory));
	}

	/**
	 * Executes {@code resultQuery} via DuckDB's native Arrow IPC stream and returns a lazy {@link Stream} of
	 * {@link ITabularRecord}.
	 *
	 * <p>
	 * All JDBC and Arrow resources are closed when the returned stream is closed.
	 */
	protected Stream<ITabularRecord> toArrowStream(QueryPod queryPod,
			ResultQuery<Record> sqlQuery,
			ITabularRecordFactory tabularRecordFactory) {
		if (queryPod.isCancelled()) {
			throw new CancelledQueryException("Query is cancelled before Arrow stream open");
		}

		String sql = sqlQuery.getSQL(ParamType.INLINED);

		// https://duckdb.org/docs/stable/clients/java#arrow-export

		// Resources collected in order so they can be closed in reverse on error or stream close
		List<AutoCloseable> resources = new ArrayList<>();

		try {
			ConnectionProvider connectionProvider = makeDsl().configuration().connectionProvider();
			Connection connection = connectionProvider.acquire();
			resources.add(() -> connectionProvider.release(connection));

			PreparedStatement stmt = connection.prepareStatement(sql);
			resources.add(stmt);

			ResultSet rs = stmt.executeQuery();
			resources.add(rs);

			DuckDBResultSet duckRs = rs.unwrap(DuckDBResultSet.class);

			Object allocator = ArrowReflection.createAllocator();
			resources.add(() -> ArrowReflection.closeAllocator(allocator));

			Object arrowReader = duckRs.arrowExportStream(allocator, duckDBParameters.getArrowBatchSize());
			resources.add(() -> ArrowReflection.closeReader(arrowReader));

			Stream<ITabularRecord> stream =
					StreamSupport.stream(new ArrowBatchSpliterator(arrowReader, tabularRecordFactory, queryPod), false);

			return stream.onClose(() -> closeAll(resources));
		} catch (RuntimeException e) {
			closeAll(resources);
			throw AdhocExceptionHelpers.wrap("Failed to open Arrow stream for table=" + getName(), e);
		} catch (SQLException e) {
			closeAll(resources);

			if (e.getMessage().contains("Binder Error")) {
				// e.g. `java.sql.SQLException: Binder Error: Referenced column "unknownColumn" not found in FROM
				// clause!`
				throw new IllegalArgumentException("Issue with columns or aggregates in table=" + getName(), e);
			} else if (e.getMessage().contains("Catalog Error")) {
				// e.g. `java.sql.SQLException: Catalog Error: Table with name someTableName does not exist!`
				throw new IllegalArgumentException("Issue with table in table=" + getName(), e);
			} else {
				throw new IllegalStateException("Failed to open Arrow stream for table=" + getName(), e);
			}
		} catch (Exception e) {
			closeAll(resources);
			throw new IllegalArgumentException("Failed to open Arrow stream for table=" + getName(), e);
		} catch (Throwable e) {
			// catch Throwable to ensure not leaking resources
			closeAll(resources);
			throw new IllegalArgumentException("Failed to open Arrow stream for table=" + getName(), e);
		}
	}

	// -------------------------------------------------------------------------
	// Arrow reflection helpers
	// -------------------------------------------------------------------------

	/**
	 * Holds Arrow API methods resolved once via reflection. Arrow is an optional runtime dependency: the class is only
	 * initialised when first referenced (i.e. on the first Arrow-based query).
	 */
	static final class ArrowReflection {

		private ArrowReflection() {
		}

		static final Method LOAD_NEXT_BATCH;
		static final Method GET_VECTOR_SCHEMA_ROOT;
		static final Method GET_ROW_COUNT;
		static final Method GET_FIELD_VECTORS;
		static final Method GET_OBJECT;

		static {
			try {
				// module: arrow-vector
				Class<?> readerClass = Class.forName("org.apache.arrow.vector.ipc.ArrowReader");
				LOAD_NEXT_BATCH = readerClass.getMethod("loadNextBatch");
				GET_VECTOR_SCHEMA_ROOT = readerClass.getMethod("getVectorSchemaRoot");

				Class<?> rootClass = Class.forName("org.apache.arrow.vector.VectorSchemaRoot");
				GET_ROW_COUNT = rootClass.getMethod("getRowCount");
				GET_FIELD_VECTORS = rootClass.getMethod("getFieldVectors");

				Class<?> vectorClass = Class.forName("org.apache.arrow.vector.ValueVector");
				GET_OBJECT = vectorClass.getMethod("getObject", int.class);
			} catch (ClassNotFoundException | NoSuchMethodException e) {
				throw new ExceptionInInitializerError(e);
			}
		}

		static Object createAllocator() {
			// module: arrow-memory-core
			try {
				Class<?> allocatorClass = Class.forName("org.apache.arrow.memory.RootAllocator");
				return allocatorClass.getDeclaredConstructor(long.class).newInstance(Long.MAX_VALUE);
			} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
					| InvocationTargetException e) {
				throw new IllegalStateException("Failed to create Arrow RootAllocator", e);
			}
		}

		static void closeAllocator(Object allocator) {
			try {
				allocator.getClass().getMethod("close").invoke(allocator);
			} catch (Exception e) {
				log.warn("Error closing Arrow allocator", e);
			}
		}

		static void closeReader(Object reader) {
			try {
				reader.getClass().getMethod("close").invoke(reader);
			} catch (Exception e) {
				log.warn("Error closing Arrow reader", e);
			}
		}

		/**
		 * Converts Arrow-specific value types to plain Java types expected by the rest of the pipeline.
		 *
		 * <p>
		 * For example, {@code VarCharVector.getObject()} returns an {@code org.apache.arrow.vector.util.Text} instance
		 * rather than a {@link String}; this method normalises such values.
		 */
		static Object convertValue(Object value) {
			if (value == null) {
				return null;
			}
			// Arrow VarCharVector returns org.apache.arrow.vector.util.Text
			if ("org.apache.arrow.vector.util.Text".equals(value.getClass().getName())) {
				return value.toString();
			}
			if (value instanceof BigDecimal bigD && bigD.scale() <= 0) {
				// Converts int/long to BigInteger
				return bigD.toBigInteger();
			}
			return value;
		}
	}

	@Override
	protected void debugResultQuery(IJooqTableQueryFactory.QueryWithLeftover resultQuery) {
		// Default behavior is not valid as we do not have a JDBC Connection to execute the DEBUG SQL
		log.info("[DEBUG] TODO DuckDB");
	}
}
