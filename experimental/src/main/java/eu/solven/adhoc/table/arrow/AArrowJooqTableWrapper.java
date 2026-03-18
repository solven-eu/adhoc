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
package eu.solven.adhoc.table.arrow;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.conf.ParamType;

import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordFactory;
import eu.solven.adhoc.engine.cancel.CancelledQueryException;
import eu.solven.adhoc.engine.context.QueryPod;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.table.sql.JooqTableWrapper;
import eu.solven.adhoc.table.sql.JooqTableWrapperParameters;
import eu.solven.adhoc.table.sql.QueryWithLeftover;
import lombok.extern.slf4j.Slf4j;

/**
 * Extension of {@link JooqTableWrapper} that streams results through Arrow record batches.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public abstract class AArrowJooqTableWrapper extends JooqTableWrapper {
	private final int minSplitRows;

	protected AArrowJooqTableWrapper(String name, JooqTableWrapperParameters tableParameters, int minSplitRows) {
		super(name, tableParameters);
		this.minSplitRows = minSplitRows;
	}

	@Override
	protected Stream<ITabularRecord> streamTabularRecords(QueryPod queryPod,
			IGroupBy mergedGroupBy,
			QueryWithLeftover sqlQuery) {
		return sqlQuery.getQueries().stream().flatMap(oneQuery -> {
			ITabularRecordFactory tabularRecordFactory =
					makeTabularRecordFactory(queryPod, mergedGroupBy, sqlQuery, oneQuery);
			return toArrowStream(queryPod, oneQuery, tabularRecordFactory);
		});
	}

	protected Stream<ITabularRecord> toArrowStream(QueryPod queryPod,
			ResultQuery<Record> sqlQuery,
			ITabularRecordFactory tabularRecordFactory) {
		if (queryPod.isCancelled()) {
			throw new CancelledQueryException("Query is cancelled before Arrow stream open");
		}

		String sql = getSQL(sqlQuery);
		List<AutoCloseable> resources = new ArrayList<>();

		try {
			Object arrowReader = openArrowReader(sql, resources);

			Stream<ITabularRecord> stream = StreamSupport.stream(
					new ArrowBatchSpliterator(arrowReader, tabularRecordFactory, queryPod, minSplitRows),
					false);

			return stream.onClose(() -> closeAll(resources));
		} catch (SQLException e) {
			closeAll(resources);
			throw onArrowSqlException(e);
		} catch (Throwable e) {
			closeAll(resources);
			throw new IllegalArgumentException("Failed to open Arrow stream for table=" + getName(), e);
		}
	}

	protected String getSQL(ResultQuery<Record> sqlQuery) {
		return sqlQuery.getSQL(ParamType.INLINED);
	}

	protected static void closeAll(List<AutoCloseable> resources) {
		for (int i = resources.size() - 1; i >= 0; i--) {
			try {
				resources.get(i).close();
			} catch (Exception e) {
				log.warn("Error closing resource", e);
			}
		}
	}

	/**
	 * Creates the Arrow reader for the given SQL. Implementations are responsible for registering every resource
	 * involved in the creation so that {@link #closeAll(List)} can clean them up.
	 */
	protected abstract Object openArrowReader(String sql, List<AutoCloseable> resources) throws SQLException;

	/**
	 * Allows subclasses to customize how SQL exceptions are reported when opening the Arrow stream fails.
	 */
	protected RuntimeException onArrowSqlException(SQLException e) {
		return new IllegalStateException("Failed to open Arrow stream for table=" + getName(), e);
	}
}
