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
package eu.solven.adhoc.engine.tabular.optimizer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.jooq.AggregateFunction;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectJoinStep;
import org.jooq.True;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import eu.solven.adhoc.cuboid.ICuboid;
import eu.solven.adhoc.cuboid.SliceAndMeasure;
import eu.solven.adhoc.cuboid.slice.ISlice;
import eu.solven.adhoc.dataframe.column.IMultitypeMergeableColumn;
import eu.solven.adhoc.engine.IAdhocFactories;
import eu.solven.adhoc.engine.step.TableQueryStep;
import eu.solven.adhoc.engine.tabular.inducer.IInducedEvaluator;
import eu.solven.adhoc.engine.tabular.inducer.IInducedEvaluatorFactory;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.map.factory.IMapBuilderPreKeys;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.AvgAggregation;
import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.primitive.AdhocPrimitiveHelpers;
import eu.solven.adhoc.primitive.IThrowingValueReceiver;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.table.sql.AdhocJooqHelper;
import eu.solven.adhoc.table.sql.ISliceToJooqCondition;
import eu.solven.adhoc.table.sql.JooqTableQueryFactory.ConditionWithFilter;
import eu.solven.adhoc.table.sql.SliceToJooqConditionFactory;
import eu.solven.adhoc.table.sql.duckdb.DuckDBHelper;
import lombok.extern.slf4j.Slf4j;

/**
 * Evaluates an induced {@link TableQueryStep} via an in-process DuckDB engine. Provides a vectorized alternative to the
 * row-by-row Java streaming path in {@link ATableQueryFactory#evaluateInduced}.
 *
 * <p>
 * Each call materialises the inducer {@link ICuboid} into a transient in-memory DuckDB table, executes a
 * {@code SELECT cols, AGG(v) FROM t WHERE filter GROUP BY cols} query, and reads the results back into an
 * {@link IMultitypeMergeableColumn}.
 *
 * @author Benoit Lacelle
 */
@SuppressWarnings("PMD.GodClass")
@Slf4j
@Deprecated(since = "adhoc.experimental")
public class DuckDBInducedEvaluator implements IInducedEvaluator {

	// Transient table name used inside each isolated DuckDB instance
	private static final String TABLE_NAME = "adhoc_induced_tmp";

	// Column name for the measure value in the transient table
	private static final String VALUE_COL = "v";

	/**
	 * Set to {@code true} permanently when DuckDB is not found on the classpath, so subsequent calls skip immediately.
	 */
	private final AtomicBoolean duckDbUnavailable = new AtomicBoolean(false);

	/**
	 * Infrastructure dependencies injected at construction time by an {@link IInducedEvaluatorFactory}. Both are
	 * {@code null} when the no-arg constructor is used (direct calls to {@link #tryEvaluateViaDuckDB} are still
	 * supported in that case).
	 */
	private final IAdhocFactories factories;

	/**
	 * Full constructor used by {@link IInducedEvaluatorFactory} implementations.
	 */
	public DuckDBInducedEvaluator(IAdhocFactories factories) {
		this.factories = factories;
	}

	/**
	 * Try to evaluate the induced step using an in-process DuckDB instance.
	 *
	 * @param inducerValues
	 *            the data from the inducer step
	 * @param inducer
	 *            the inducer {@link TableQueryStep}
	 * @param induced
	 *            the induced {@link TableQueryStep}
	 * @param leftoverFilter
	 *            the additional filter to apply on top of the inducer data
	 * @param aggregation
	 *            the {@link IAggregation} for the output column
	 * @param aggregator
	 *            the {@link Aggregator} describing the aggregation key
	 * @return {@link Optional#empty()} when the DuckDB path is not applicable or not available; otherwise the computed
	 *         column.
	 */
	@Override
	public Optional<IMultitypeMergeableColumn<ISlice>> tryEvaluate(ICuboid inducerValues,
			TableQueryStep inducer,
			TableQueryStep induced,
			ISliceFilter leftoverFilter,
			IAggregation aggregation,
			Aggregator aggregator) {

		if (duckDbUnavailable.get()) {
			return Optional.empty();
		}

		// Guard: only handle standard SQL-compatible aggregations
		String aggKey = aggregator.getAggregationKey();
		if (!isSupportedAggregation(aggKey)) {
			return Optional.empty();
		}

		// Guard: nothing to process for an empty input
		if (inducerValues.isEmpty()) {
			return Optional.empty();
		}

		try {
			return evaluateViaDuckDB(factories, inducerValues, inducer, induced, leftoverFilter, aggregation, aggKey);
		} catch (NoClassDefFoundError e) {
			log.warn("DuckDB is not on the classpath — disabling DuckDB-backed evaluateInduced path", e);
			duckDbUnavailable.set(true);
			return Optional.empty();
		} catch (RuntimeException e) {
			log.warn("DuckDB evaluateInduced path failed; falling back to Java stream. inducer={} induced={}",
					inducer,
					induced,
					e);
			return Optional.empty();
		}
	}

	protected boolean isSupportedAggregation(String aggKey) {
		return SumAggregation.KEY.equals(aggKey) || MaxAggregation.KEY.equals(aggKey)
				|| MinAggregation.KEY.equals(aggKey)
				|| AvgAggregation.isAvg(aggKey)
				|| CountAggregation.isCount(aggKey);
	}

	protected Optional<IMultitypeMergeableColumn<ISlice>> evaluateViaDuckDB(IAdhocFactories factories,
			ICuboid inducerValues,
			TableQueryStep inducer,
			TableQueryStep induced,
			ISliceFilter leftoverFilter,
			IAggregation aggregation,
			String aggKey) {

		NavigableSet<String> inducerGroupByCols = inducer.getGroupBy().getSortedColumns();
		NavigableSet<String> inducedGroupByCols = induced.getGroupBy().getSortedColumns();

		// Probe the first entry to determine schema types
		Optional<SliceAndMeasure<ISlice>> optFirst = inducerValues.stream().findAny();
		if (optFirst.isEmpty()) {
			return Optional.empty();
		}
		SliceAndMeasure<ISlice> firstEntry = optFirst.get();
		ISlice firstSlice = firstEntry.getSlice();

		DataType<?> valueDataType = probeValueDataType(firstEntry);

		// Each call gets its own isolated in-memory DuckDB instance
		// https://duckdb.org/docs/api/java.html
		try (DuckDBConnection conn = DuckDBHelper.makeFreshInMemoryDb()) {
			DSLContext dsl = DSL.using(conn, SQLDialect.DUCKDB);

			// ── 1. CREATE TABLE ──────────────────────────────────────────────────────
			List<Field<?>> columnDefs = buildColumnDefs(inducerGroupByCols, firstSlice, valueDataType);
			dsl.createTable(TABLE_NAME).columns(columnDefs).execute();

			// ── 2. Bulk-insert via DuckDBAppender ────────────────────────────────────
			// https://duckdb.org/docs/api/java.html#appender
			bulkInsert(conn, inducerValues);

			// ── 3. Build SELECT, filter it and execute ───────────────────────────────
			Function<String, org.jooq.Name> toName = col -> AdhocJooqHelper.name(col, dsl::parser);
			ISliceToJooqCondition sliceToJooq = new SliceToJooqConditionFactory().with(toName);
			ConditionWithFilter conditionWithFilter = sliceToJooq.toConditionSplitLeftover(leftoverFilter);

			// If some filter predicates cannot be expressed in SQL, fall back to Java
			if (!conditionWithFilter.getLeftover().isMatchAll()) {
				log.debug("DuckDB evaluator: filter has SQL leftover; falling back to Java. leftover={}",
						conditionWithFilter.getLeftover());
				return Optional.empty();
			}

			Result<Record> records = fetchRecords(dsl, toName, inducedGroupByCols, aggKey, conditionWithFilter);

			// ── 4. Read results into IMultitypeMergeableColumn ───────────────────────
			IMultitypeMergeableColumn<ISlice> result =
					collectResults(records, inducedGroupByCols, aggregation, firstSlice.getFactory());

			log.debug("DuckDB evaluateInduced: inducer.size={} result.size={} for inducer={} induced={}",
					inducerValues.size(),
					result.size(),
					inducer,
					induced);
			return Optional.of(result);

		} catch (SQLException e) {
			throw new IllegalStateException("DuckDB evaluation failed", e);
		}
	}

	protected static DataType<?> probeValueDataType(SliceAndMeasure<ISlice> firstEntry) {
		boolean[] valueIsLong = { false };
		boolean[] valueIsDouble = { false };
		firstEntry.getValueProvider().acceptReceiver(new IValueReceiver() {
			@Override
			public void onLong(long v) {
				valueIsLong[0] = true;
			}

			@Override
			public void onDouble(double v) {
				valueIsDouble[0] = true;
			}

			@Override
			public void onObject(Object v) {
				// Both flags remain false → we will use VARCHAR
			}
		});
		if (valueIsLong[0]) {
			return SQLDataType.BIGINT;
		} else if (valueIsDouble[0]) {
			return SQLDataType.DOUBLE;
		} else {
			return SQLDataType.VARCHAR;
		}
	}

	protected List<Field<?>> buildColumnDefs(NavigableSet<String> inducerGroupByCols,
			ISlice firstSlice,
			DataType<?> valueDataType) {
		List<Field<?>> columnDefs = new ArrayList<>();
		for (String col : inducerGroupByCols) {
			DataType<?> colType = javaToSqlDataType(firstSlice.asAdhocMap().get(col));
			columnDefs.add(DSL.field(DSL.quotedName(col), colType));
		}
		columnDefs.add(DSL.field(DSL.quotedName(VALUE_COL), valueDataType));
		return columnDefs;
	}

	protected void bulkInsert(DuckDBConnection conn, ICuboid inducerValues) {
		// DuckDBAppender.close() flushes any buffered data
		// https://duckdb.org/docs/api/java.html#appender
		try (DuckDBAppender appender = conn.createAppender(TABLE_NAME)) {
			inducerValues.forEachSlice(slice -> {
				try {
					appender.beginRow();

					slice.forEachGroupBy((column, coordinate) -> {
						try {
							appendToAppender(appender, coordinate);
						} catch (SQLException e) {
							throw new IllegalStateException(
									"Issue on column=%s coordinate=%s".formatted(column, coordinate),
									e);
						}
					});
					return appendValueToAppender(appender);
				} catch (SQLException e) {
					throw new IllegalStateException("Issue on slice=%s".formatted(slice), e);
				}
			});
		} catch (SQLException e) {
			throw new IllegalStateException("DuckDB bulk insert failed", e);
		}
	}

	protected Result<Record> fetchRecords(DSLContext dsl,
			Function<String, org.jooq.Name> toName,
			NavigableSet<String> inducedGroupByCols,
			String aggKey,
			ConditionWithFilter conditionWithFilter) {
		List<Field<?>> groupByFields = new ArrayList<>();
		for (String col : inducedGroupByCols) {
			groupByFields.add(DSL.field(toName.apply(col)));
		}

		Field<Object> valueField = DSL.field(DSL.quotedName(VALUE_COL));
		AggregateFunction<?> aggFunction = toAggFunction(aggKey, valueField);

		List<org.jooq.SelectFieldOrAsterisk> selectFields = new ArrayList<>(groupByFields);
		selectFields.add(aggFunction.as(VALUE_COL));

		SelectJoinStep<Record> selectFrom = dsl.select(selectFields).from(TABLE_NAME);

		Condition whereCondition = conditionWithFilter.getCondition();
		if (whereCondition instanceof True) {
			return selectFrom.groupBy(groupByFields).fetch();
		} else {
			return selectFrom.where(whereCondition).groupBy(groupByFields).fetch();
		}
	}

	protected IMultitypeMergeableColumn<ISlice> collectResults(Result<Record> records,
			NavigableSet<String> inducedGroupByCols,
			IAggregation aggregation,
			ISliceFactory sliceFactory) {
		IMultitypeMergeableColumn<ISlice> result =
				factories.getColumnFactory().makeColumnRandomInsertions(aggregation, records.size());

		int inducedColCount = inducedGroupByCols.size();
		for (Record record : records) {
			IMapBuilderPreKeys builder = sliceFactory.newMapBuilder(inducedGroupByCols);
			for (int i = 0; i < inducedColCount; i++) {
				builder = builder.append(record.get(i));
			}
			ISlice slice = builder.build().asSlice();

			// The aggregated value is in the last column (index = inducedColCount)
			Object aggValue = record.get(inducedColCount);
			if (aggValue == null) {
				// DuckDB SUM/AVG of all-NULL inputs → NULL: skip
				continue;
			}

			IValueReceiver receiver = result.merge(slice);

			if (aggregation instanceof IAggregationCarrier.IHasCarriers hasCarriers) {
				aggValue = hasCarriers.wrap(aggValue);
			}

			mergeValue(receiver, aggValue);
		}

		return result;
	}

	protected DataType<?> javaToSqlDataType(Object sample) {
		if (sample == null) {
			return SQLDataType.VARCHAR;
		} else if (sample instanceof Long || sample instanceof Integer
				|| sample instanceof Short
				|| sample instanceof Byte) {
			return SQLDataType.BIGINT;
		} else if (sample instanceof Double || sample instanceof Float) {
			return SQLDataType.DOUBLE;
		} else {
			return SQLDataType.VARCHAR;
		}
	}

	protected void appendToAppender(DuckDBAppender appender, Object val) throws SQLException {
		if (val == null) {
			appender.appendNull();
		} else if (val instanceof Long l) {
			appender.append(l);
		} else if (val instanceof Integer i) {
			appender.append((long) i);
		} else if (val instanceof Double d) {
			appender.append(d);
		} else if (val instanceof Float f) {
			appender.append((double) f);
		} else {
			appender.append(val.toString());
		}
	}

	protected IThrowingValueReceiver appendValueToAppender(DuckDBAppender appender) {
		return new IThrowingValueReceiver() {

			@Override
			public void onLongMayThrow(long value) throws SQLException {
				appender.append(value);
				appender.endRow();
			}

			@Override
			public void onDoubleMayThrow(double value) throws SQLException {
				appender.append(value);
				appender.endRow();
			}

			@Override
			public void onObjectMayThrow(Object value) throws SQLException {
				if (value == null) {
					appender.appendNull();
				} else {
					appender.append(value.toString());
				}
				appender.endRow();
			}
		};
	}

	// see JooqTableQueryFactory.toSqlAggregatedColumn(ISliceToJooqCondition, FilteredAggregator)
	protected AggregateFunction<?> toAggFunction(String aggKey, Field<Object> field) {
		if (SumAggregation.KEY.equals(aggKey)) {
			// https://duckdb.org/docs/stable/sql/functions/aggregates.html#sumarg
			return DSL.aggregate("sum", Object.class, field);
		} else if (MaxAggregation.KEY.equals(aggKey)) {
			return DSL.max(field);
		} else if (MinAggregation.KEY.equals(aggKey)) {
			return DSL.min(field);
		} else if (AvgAggregation.isAvg(aggKey)) {
			// https://duckdb.org/docs/stable/sql/functions/aggregates.html#avgarg
			return DSL.aggregate("avg", Object.class, field);
		} else if (CountAggregation.isCount(aggKey)) {
			return DSL.count(field);
		} else {
			throw new IllegalArgumentException("Unsupported aggregation for DuckDB path: " + aggKey);
		}
	}

	/**
	 * Translates a value returned by DuckDB JDBC (which may be {@link BigDecimal} or {@link BigInteger} for integer
	 * aggregations) into the appropriate {@link IValueReceiver} callback.
	 *
	 * @param receiver
	 *            the receiver to notify
	 * @param aggValue
	 *            the non-null value from the jOOQ {@link Record}
	 */
	protected void mergeValue(IValueReceiver receiver, Object aggValue) {
		AdhocPrimitiveHelpers.normalizeValueAsProvider(aggValue).acceptReceiver(receiver);
	}
}
