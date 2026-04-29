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
package eu.solven.adhoc.table.sql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.jooq.AggregateFunction;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.GroupField;
import org.jooq.Name;
import org.jooq.OrderField;
import org.jooq.Param;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.SelectConnectByStep;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.SelectHavingStep;
import org.jooq.SelectJoinStep;
import org.jooq.SortField;
import org.jooq.TableLike;
import org.jooq.True;
import org.jooq.impl.DSL;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.dataframe.row.AggregatedRecordFields;
import eu.solven.adhoc.filter.AdhocFilterUnsafe;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.optimizer.IFilterOptimizer;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.measure.sum.AvgAggregation;
import eu.solven.adhoc.measure.sum.CoalesceAggregation;
import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.sum.ExpressionAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.groupby.IHasSqlExpression;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.query.table.TableQueryV3;
import eu.solven.adhoc.query.table.TableQueryV4;
import eu.solven.adhoc.query.top.AdhocTopClause;
import eu.solven.adhoc.table.transcoder.AliasingContext;
import eu.solven.adhoc.table.transcoder.ITableAliaser;
import eu.solven.adhoc.util.IHasName;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * This is especially important to make sure all calls to {@link ITableAliaser} relies on a {@link AliasingContext}
 *
 * @author Benoit Lacelle
 */
@SuperBuilder
@Slf4j
@SuppressWarnings({ "PMD.GodClass", "PMD.CouplingBetweenObjects" })
public class JooqTableQueryFactory implements IJooqTableQueryFactory {

	@NonNull
	@Builder.Default
	final IOperatorFactory operatorFactory = StandardOperatorFactory.builder().build();

	/**
	 * Optional per-query table provider. When set, {@link #prepareSliceQuery(TableQueryV4)} substitutes the
	 * {@link #table} field with {@link IJooqTableSupplier#tableFor(TableQueryV4)}. When {@code null}, the constant
	 * {@link #table} is always used (current behaviour).
	 */
	@NonNull
	final IJooqTableSupplier tableSupplier;

	@NonNull
	final DSLContext dslContext;

	@Getter(AccessLevel.PACKAGE)
	final JooqTableCapabilities capabilities;

	@NonNull
	@Builder.Default
	final SliceToJooqConditionFactory sliceToCondition = new SliceToJooqConditionFactory();

	/**
	 * Query-scoped optimizer forwarded to every {@link SliceToJooqCondition} instance produced by
	 * {@link #makeToCondition()}. Defaults to {@link AdhocFilterUnsafe#filterOptimizer}.
	 */
	@NonNull
	@Builder.Default
	IFilterOptimizer filterOptimizer = AdhocFilterUnsafe.filterOptimizer;

	@NonNull
	@Default
	final IQueryPartitionor queryPartitionor = IQueryPartitionor.SINGLE_PARTITION;

	/**
	 * Manually-declared inner builder class. Lombok's {@link lombok.experimental.SuperBuilder} merges the
	 * auto-generated setters and fields with the members declared here; the only member we declare is the migration
	 * helper {@link #table(TableLike)}.
	 */
	@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
	public abstract static class JooqTableQueryFactoryBuilder<C extends JooqTableQueryFactory,
			B extends JooqTableQueryFactoryBuilder<C, B>> {
		/**
		 * Migration helper: accepts a constant {@link TableLike} and wires it as
		 * {@code tableSupplier(IJooqTableSupplier.constant(table))}. Prefer {@link #tableSupplier(IJooqTableSupplier)}
		 * directly when the {@code FROM} clause must vary per query.
		 *
		 * @param table
		 *            the constant {@link TableLike} to place in the {@code FROM} clause
		 * @return this builder, for chaining
		 */
		public B table(TableLike<?> table) {
			return this.tableSupplier(IJooqTableSupplier.constant(table));
		}
	}

	// BEWARE This is Delombokized. It customizes the case `capabilities == null`
	protected JooqTableQueryFactory(JooqTableQueryFactoryBuilder<?, ?> b) {
		if (b.operatorFactory$set) {
			this.operatorFactory = b.operatorFactory$value;
		} else {
			this.operatorFactory = $default$operatorFactory();
		}
		this.tableSupplier = b.tableSupplier;
		this.dslContext = b.dslContext;

		if (b.capabilities == null) {
			this.capabilities = JooqTableCapabilities.from(dslContext.dialect());
		} else {
			this.capabilities = b.capabilities;
		}
		if (b.sliceToCondition$set) {
			this.sliceToCondition = b.sliceToCondition$value;
		} else {
			this.sliceToCondition = $default$sliceToCondition();
		}
		if (b.queryPartitionor$set) {
			this.queryPartitionor = b.queryPartitionor$value;
		} else {
			this.queryPartitionor = $default$queryPartitionor();
		}
		if (b.filterOptimizer$set) {
			this.filterOptimizer = b.filterOptimizer$value;
		} else {
			this.filterOptimizer = $default$filterOptimizer();
		}
	}

	/**
	 * Holds a Set of SQL {@link Condition}s, given an {@link ISliceFilter}. Some filters may not be convertible into
	 * SQL. In such case, we ensure the columns are in the groupBy for manual filtering.
	 * 
	 * Both conditions can be considered as being ANDed together.
	 */
	@Value
	@Builder
	public static class ConditionWithFilter {
		// SQL conditions, translated from an IAdhocFilter
		@NonNull
		@Builder.Default
		Condition condition = DSL.trueCondition();
		// Holds the filter of the conditions which were not translated into SQL
		@NonNull
		@Builder.Default
		ISliceFilter leftover = ISliceFilter.MATCH_ALL;
	}

	@Deprecated
	public QueryWithLeftover prepareSliceQuery(TableQueryV2 tableQuery) {
		return prepareSliceQuery(TableQueryV3.edit(tableQuery).build());
	}

	@Override
	public QueryWithLeftover prepareSliceQuery(TableQueryV4 tableQuery) {
		// TODO We should do a UNION ALL and not a covering V3
		TableQueryV3 v3 = tableQuery.asCoveringV3();

		if (tableQuery.isDebugOrExplain()) {
			long nbEvaluatedTableInducers = TableQueryV3.nbCuboids(v3);
			long tableStepsCount = tableQuery.streamV3().mapToLong(TableQueryV3::nbCuboids).sum();

			// prints percent with 1 digit.
			String percentEfficiency = percent(tableStepsCount, nbEvaluatedTableInducers);
			log.info("[EXPLAIN] {} inducers evaluated by {} tableQuery (evaluating {} steps). Efficiency={} v3={}",
					tableStepsCount,
					1,
					nbEvaluatedTableInducers,
					percentEfficiency,
					v3);
		}
		return prepareSliceQuery(v3, resolveTable(tableQuery));
	}

	@SuppressWarnings("checkstyle:MagicNumber")
	protected String percent(long numerator, long denominator) {
		return "%.1f%%".formatted(100.0 * numerator / denominator);
	}

	/**
	 * Resolves the {@link TableLike} to use in the {@code FROM} clause for a given query. Defaults to the constant
	 * {@link #table} unless an {@link IJooqTableSupplier} was wired via the builder, in which case it is consulted.
	 * Subclasses may override to plug in their own per-query logic.
	 */
	protected TableLike<?> resolveTable(TableQueryV4 tableQuery) {
		return tableSupplier.tableFor(tableQuery);
	}

	protected QueryWithLeftover prepareSliceQuery(TableQueryV3 tableQuery) {
		return prepareSliceQuery(tableQuery, resolveTable(TableQueryV4.edit(tableQuery).build()));
	}

	/**
	 * SLICES variant: GROUP BY + aggregate functions. Wraps each aggregator in its SQL aggregation function (e.g.
	 * {@code SUM(col) FILTER (WHERE ...)}) and applies a {@code GROUP BY} on the requested columns. One row per
	 * distinct slice.
	 */
	protected QueryWithLeftover prepareSliceQuery(TableQueryV3 tableQuery, TableLike<?> fromTable) {
		return prepareQuery(tableQuery, fromTable, SqlRenderMode.SLICES);
	}

	/**
	 * ROWS variant: no GROUP BY, no aggregate function. Each per-aggregator FILTER becomes a
	 * {@code CASE WHEN <filter> THEN <column> END AS <alias>} (the column is null when the FILTER does not match), so
	 * each surviving DB row produces one record. This is the foundation of
	 * {@link eu.solven.adhoc.options.StandardQueryOptions#DRILLTHROUGH}.
	 *
	 * @param tableQuery
	 *            the merged DRILLTHROUGH query.
	 * @return the {@link QueryWithLeftover} carrying the raw-rows SQL.
	 */
	@Override
	public QueryWithLeftover prepareRowsQuery(TableQueryV3 tableQuery) {
		return prepareQuery(tableQuery, resolveTable(TableQueryV4.edit(tableQuery).build()), SqlRenderMode.ROWS);
	}

	/**
	 * SQL rendering mode, capturing the only two axes by which {@link #prepareSliceQuery} and {@link #prepareRowsQuery}
	 * differ:
	 * <ul>
	 * <li>{@link #SLICES}: SELECT wraps each aggregator in its SQL aggregation function and a {@code GROUP BY} clause
	 * is emitted over the requested groupBy columns.</li>
	 * <li>{@link #ROWS}: SELECT emits a {@code CASE WHEN <filter> THEN <column>} per FA (no aggregation function) and
	 * no {@code GROUP BY} is emitted, so each surviving DB row produces one record.</li>
	 * </ul>
	 * Everything else (WHERE, FROM, leftover splitting, ORDER BY, partitioning, QueryWithLeftover assembly) is shared.
	 */
	protected enum SqlRenderMode {
		SLICES, ROWS
	}

	/**
	 * Shared scaffold for both {@link #prepareSliceQuery} and {@link #prepareRowsQuery}. The two methods only differ on
	 * how they render the SELECT clause (aggregate-functions-with-FILTER vs CASE-WHEN) and whether they emit a
	 * {@code GROUP BY} — both axes captured by {@link SqlRenderMode}.
	 */
	protected QueryWithLeftover prepareQuery(TableQueryV3 tableQuery, TableLike<?> fromTable, SqlRenderMode mode) {
		ISliceToJooqCondition toCondition = makeToCondition();

		ConditionWithFilter conditionAndLeftover = toConditions(toCondition, tableQuery);

		// Leftover in FILTER clause — common to both modes: any FA whose FILTER cannot be transcoded fully into
		// SQL records its leftover here, and the JooqTableWrapper applies the leftover post-fetch.
		Map<String, ISliceFilter> aggregateToLeftover = new LinkedHashMap<>();
		tableQuery.getAggregators().forEach(filtered -> {
			ConditionWithFilter conditionWithFilter = toCondition.toConditionSplitLeftover(filtered.getFilter());
			if (!conditionWithFilter.getLeftover().isMatchAll()) {
				aggregateToLeftover.put(filtered.getAlias(), conditionWithFilter.getLeftover());
			}
		});

		ImmutableSet<ISliceFilter> leftovers = ImmutableSet.<ISliceFilter>builder()
				.add(conditionAndLeftover.getLeftover())
				.addAll(aggregateToLeftover.values())
				.build();
		AggregatedRecordFields fields = selectedColumns(tableQuery, leftovers);

		// `SELECT ...` — the FIRST mode-specific axis.
		Collection<SelectFieldOrAsterisk> selectedFields = switch (mode) {
		case SLICES -> selectedSliceFields(toCondition, tableQuery, fields);
		case ROWS -> selectedRowsFields(toCondition, tableQuery, fields);
		};

		// `FROM ...`
		SelectJoinStep<Record> selectFrom = dslContext.select(selectedFields).from(fromTable);

		// `WHERE ...`
		SelectConnectByStep<Record> selectFromWhere;
		if (conditionAndLeftover.getCondition() instanceof True) {
			selectFromWhere = selectFrom;
		} else {
			selectFromWhere = selectFrom.where(conditionAndLeftover.getCondition());
		}

		// `GROUP BY ...` — the SECOND mode-specific axis. ROWS emits no GROUP BY at all.
		// TODO We may like to break `GROUPING SET` around here (see TableQueryV4.streamV3 and
		// JooqTableWrapper.streamSlices). TableQueryV4.isPerfectV3() is the shared flag that signals
		// whether a covering GROUPING SET is efficient (true) or wasteful (false). Three strategies
		// worth exposing as a configurable option:
		//
		// 1. GROUPING SETS (current default via asCoveringV3): one SQL query, cartesian product of all
		// (groupBy × aggregator) pairs. Efficient when isPerfectV3() is true; wasteful otherwise,
		// because it computes irrelevant (groupBy, aggregator) combinations.
		//
		// 2. UNION ALL via multiple TableQueryV3 (TableQueryV4.streamV3): one SQL per distinct aggregator
		// set; each query covers only the (groupBy, aggregator) pairs that actually need each other.
		// Avoids cartesian waste but adds per-query overhead. Preferable when isPerfectV3() is false.
		//
		// 3. Literal SQL UNION ALL (not yet implemented): a single SQL statement whose branches are
		// UNION ALL'd by the DB engine itself. Unlike option 2 (which concatenates at the Java level),
		// this lets the DB share one scan across branches and can be faster on columnar engines.
		//
		// The right choice depends on the DB engine, the scale factor, and the degree of aggregator-set
		// overlap. Benchmark with TestTableQuery_DuckDb_Tpch.testGroupingSets_vs_UnionAll_* to decide.
		ResultQuery<Record> beforeOrder = switch (mode) {
		case SLICES -> selectFromWhere.groupBy(makeGroupingFields(tableQuery, conditionAndLeftover.getLeftover()));
		case ROWS -> selectFromWhere;
		};

		// `ORDER BY ...` / `LIMIT ...`
		ResultQuery<Record> resultQuery;
		if (tableQuery.getTopClause().isPresent()) {
			Collection<? extends OrderField<?>> optOrderFields = getOptionalOrders(tableQuery);
			resultQuery = applyOrderAndLimit(beforeOrder, optOrderFields, tableQuery.getTopClause().getLimit());
		} else {
			resultQuery = beforeOrder;
		}

		return QueryWithLeftover.builder()
				.queries(partitionQuery(resultQuery))
				.leftover(conditionAndLeftover.getLeftover())
				.aggregatorToLeftovers(aggregateToLeftover)
				.fields(fields)
				.build();
	}

	/**
	 * SELECT-fields builder for {@link SqlRenderMode#ROWS}: one {@code CASE WHEN <filter> THEN <col>} per FA, plus the
	 * groupBy columns as plain fields, plus any leftover columns. Empty aggregators (slice-materialization aids) are
	 * dropped — they have no SQL counterpart in row-streaming mode.
	 */
	protected Collection<SelectFieldOrAsterisk> selectedRowsFields(ISliceToJooqCondition toCondition,
			TableQueryV3 tableQuery,
			AggregatedRecordFields fields) {
		List<SelectFieldOrAsterisk> selectedFields = new ArrayList<>();
		tableQuery.getAggregators().stream().distinct().forEach(filteredAggregator -> {
			Aggregator a = filteredAggregator.getAggregator();
			if (EmptyAggregation.isEmpty(a.getAggregationKey())) {
				return;
			}
			Field<Object> rawColumn = DSL.field(name(a.getColumnName()));
			ConditionWithFilter faCondition = toCondition.toConditionSplitLeftover(filteredAggregator.getFilter());
			Field<Object> withCase = asCase(faCondition.getCondition(), rawColumn);
			selectedFields.add(withCase.as(filteredAggregator.getAlias()));
		});
		Map<String, IAdhocColumn> distinctColumns = tableQuery.getColumns();
		fields.getColumns().stream().map(distinctColumns::get).forEach(column -> {
			Field<Object> field = columnAsField(column);
			selectedFields.add(field);
		});
		fields.getLeftovers().forEach(leftover -> {
			Field<Object> field = columnAsField(ReferencedColumn.ref(leftover));
			selectedFields.add(field);
		});
		if (selectedFields.isEmpty()) {
			// No aggregator and no groupBy: still emit a row marker so DRILLTHROUGH counts the matching rows.
			selectedFields.add(DSL.val(1));
		}
		return selectedFields;
	}

	/**
	 * Apply ORDER BY + LIMIT to the query. Extracted so the SLICES (post-GROUP BY) and ROWS (post-WHERE) branches share
	 * the same call site even though their input types differ in JOOQ's fluent API.
	 */
	protected ResultQuery<Record> applyOrderAndLimit(ResultQuery<Record> resultQuery,
			Collection<? extends OrderField<?>> orderFields,
			Number limit) {
		if (resultQuery instanceof SelectHavingStep<Record> havingStep) {
			return havingStep.orderBy(orderFields).limit(limit);
		} else if (resultQuery instanceof SelectConnectByStep<Record> connectStep) {
			return connectStep.orderBy(orderFields).limit(limit);
		} else {
			throw new IllegalStateException(
					"Unsupported jOOQ query stage for ORDER BY/LIMIT: %s".formatted(resultQuery.getClass().getName()));
		}
	}

	protected ISliceToJooqCondition makeToCondition() {
		return sliceToCondition.with(this::name, filterOptimizer);
	}

	protected List<ResultQuery<Record>> partitionQuery(ResultQuery<Record> resultQuery) {
		return queryPartitionor.partition(resultQuery);
	}

	protected ConditionWithFilter toConditions(ISliceToJooqCondition toCondition, TableQueryV3 tableQuery) {
		Collection<Condition> conditions = new ArrayList<>();
		Collection<ISliceFilter> leftoverFilters = new ArrayList<>();

		// Conditions from filters
		{
			ISliceFilter filter = tableQuery.getFilter();
			ConditionWithFilter conditionWithFilter = toCondition.toConditionSplitLeftover(filter);

			conditions.add(conditionWithFilter.getCondition());
			leftoverFilters.add(conditionWithFilter.getLeftover());
		}

		// AND conditions from measures and from filters
		return makeToCondition().and(conditions, leftoverFilters);
	}

	protected List<SelectFieldOrAsterisk> selectedSliceFields(ISliceToJooqCondition toCondition,
			TableQueryV3 tableQuery,
			AggregatedRecordFields fields) {
		List<SelectFieldOrAsterisk> selectedFields = new ArrayList<>();
		tableQuery.getAggregators().stream().distinct().map(a -> {
			try {
				return toSqlAggregatedColumn(toCondition, a);
			} catch (RuntimeException e) {
				throw new IllegalArgumentException("Issue converting to SQL: %s".formatted(a), e);
			}
		})
				// EmptyAggregation leads to no SQL aggregation
				.filter(Objects::nonNull)
				.forEach(selectedFields::add);

		// Distinct as `GROUPING SET` typically leads to a column to appear multiple times
		Map<String, IAdhocColumn> distinctColumns = tableQuery.getColumns();

		fields.getColumns().stream().map(distinctColumns::get).forEach(column -> {
			Field<Object> field = columnAsField(column);
			selectedFields.add(field);
		});

		// TODO Should the leftover be also added in `.makeGroupingFields`?
		fields.getLeftovers().forEach(leftover -> {
			Field<Object> field = columnAsField(ReferencedColumn.ref(leftover));
			selectedFields.add(field);
		});

		// https://learn.microsoft.com/en-us/sql/t-sql/functions/grouping-transact-sql?view=sql-server-ver17
		// https://docs.aws.amazon.com/redshift/latest/dg/r_GROUP_BY_aggregation-extensions.html#r_GROUP_BY_aggregation-extentions-grouping
		// https://neon.com/postgresql/postgresql-tutorial/postgresql-grouping-sets#grouping-function
		fields.getGroupingColumns()
				.stream()
				// alias else jooq would name `grouping` leading to ambiguities
				.map(column -> DSL.grouping(columnAsField(ReferencedColumn.ref(column))).as(groupingAlias(column)))
				.forEach(selectedFields::add);

		if (selectedFields.isEmpty()) {
			// Typically happens on EmptyAggregation on grandTotal
			// We force one field to prevent JooQ querying automatically for `*`
			// BEWARE Rely on `count(1)` and not `1`, else DuckDB considers all fields are requested, and the groupBy
			// lists all rows.
			selectedFields.add(DSL.aggregate("count", long.class, DSL.val(1)));
		}

		return selectedFields;
	}

	/**
	 * @param tableQuery
	 *            the initial tableQuery
	 * @param leftovers
	 *            the filter which has to be applied manually over the output slices (e.g. on a customFilter which can
	 *            not be transcoded for given table). As a set as there may be a leftover on the common `WHERE` clause,
	 *            and on each `FILTER` clause.
	 * @return the {@link List} of the columns to be output by the tableQuery
	 */
	// BEWARE Is this a JooQ specific logic?
	public static AggregatedRecordFields makeSelectedColumns(TableQueryV2 tableQuery, Set<ISliceFilter> leftovers) {
		List<String> aggregatorNames = tableQuery.getAggregators()
				.stream()
				.distinct()
				.filter(a -> !EmptyAggregation.isEmpty(a.getAggregator().getAggregationKey()))
				.map(FilteredAggregator::getAlias)
				.toList();

		List<String> groupByColumns = tableQuery.getGroupBy().getColumns().stream().map(IHasName::getName).toList();

		List<String> leftoversColumns = new ArrayList<>(
				leftovers.stream().flatMap(leftover -> FilterHelpers.getFilteredColumns(leftover).stream()).toList());

		// Make sure a late column is not also a normal groupBy column
		leftoversColumns.removeAll(groupByColumns);

		return AggregatedRecordFields.builder()
				.aggregates(aggregatorNames)
				.columns(groupByColumns)
				.leftovers(leftoversColumns)
				.build();
	}

	protected AggregatedRecordFields selectedColumns(TableQueryV3 tableQuery, Set<ISliceFilter> leftovers) {
		return QueryWithLeftover.makeSelectedColumns(tableQuery, leftovers);
	}

	/**
	 * Most usages are not groupBy.
	 *
	 * @param column
	 * @return a {@link Field} mapping to given column.
	 */
	protected Field<Object> columnAsField(IAdhocColumn column) {
		String columnName = column.getName();
		Field<Object> field;

		if (column instanceof IHasSqlExpression hasSql) {
			// TODO How could we transcode column referred by the SQL?
			// Should we add named columns from transcoder?
			String sql = hasSql.getSql();
			field = DSL.field(sql).as(columnName);
		} else {
			Field<Object> unaliasedField = DSL.field(name(columnName));

			// GroupBy: refer to the underlying column, to prevent ambiguities
			// If we were to have some aliasing around here, aliases should probably not be applied on groupBy
			// https://github.com/duckdb/duckdb/issues/16097
			// https://github.com/jOOQ/jOOQ/issues/17980
			field = unaliasedField;
		}
		return field;
	}

	/**
	 *
	 * @param name
	 *            may be a simple columnName (e.g. `someField`), or a joined field (e.g. `someTable.someField`),or a
	 *            qualified name (e.g. `"someTable.someField"`).
	 * @return
	 */
	protected Name name(String name) {
		return AdhocJooqHelper.name(name, dslContext::parser);
	}

	/**
	 *
	 * @param tableQuery
	 * @param leftoverFilter
	 *            the filter which has not been able to be transcoded into a {@link Condition}
	 * @return
	 */
	protected Collection<GroupField> makeGroupingFields(TableQueryV3 tableQuery, ISliceFilter leftoverFilter) {
		List<GroupField> groupedFields = new ArrayList<>();
		if (tableQuery.singleGroupBy().isPresent()) {
			if (canGroupByAll()) {
				// `GROUP BY ALL` is supported by: DuckDB, RedShift, More?
				// https://duckdb.org/docs/stable/sql/query_syntax/groupby.html#group-by-all
				// https://docs.aws.amazon.com/redshift/latest/dg/r_GROUP_BY_clause.html
				groupedFields.add(DSL.field(DSL.unquotedName("ALL")));
			} else {
				tableQuery.getColumns().values().forEach(column -> {
					Field<Object> field = columnAsField(column);
					groupedFields.add(field);
				});

				FilterHelpers.getFilteredColumns(leftoverFilter).forEach(column -> {
					Field<Object> field = columnAsField(ReferencedColumn.ref(column));
					groupedFields.add(field);
				});
			}
		} else {
			// At least 2 groupingSets

			List<? extends List<? extends Field<?>>> fields2 = tableQuery.streamGroupBy().map(gb -> {
				return gb.getColumns().stream().map(this::columnAsField).toList();
			}).toList();

			Collection<? extends Field<?>>[] fieldSets = fields2.toArray(List[]::new);

			groupedFields.add(DSL.groupingSets(fieldSets));
		}

		return groupedFields;
	}

	protected boolean canGroupByAll() {
		return capabilities.isAbleToGroupByAll();
	}

	protected boolean canFilterAggregates() {
		return capabilities.isAbleToFilterAggregates();
	}

	protected List<? extends OrderField<?>> getOptionalOrders(TableQueryV3 tableQuery) {
		AdhocTopClause topClause = tableQuery.getTopClause();
		return topClause.getColumns().stream().map(c -> {
			Field<Object> field = columnAsField(c);

			SortField<Object> desc;
			if (topClause.isDesc()) {
				desc = field.desc();
			} else {
				desc = field.asc();
			}

			return desc;
		}).toList();
	}

	protected SelectFieldOrAsterisk toSqlAggregatedColumn(ISliceToJooqCondition toCondition,
			FilteredAggregator filteredAggregator) {
		Aggregator a = filteredAggregator.getAggregator();

		String aggregationKey = a.getAggregationKey();
		if (EmptyAggregation.isEmpty(aggregationKey)) {
			// There is no aggregation for empty: we just want to fetch groupBys
			return null;
		}

		String columnName = a.getColumnName();

		if (ExpressionAggregation.isExpression(aggregationKey)) {
			return buildExpressionField(columnName, filteredAggregator);
		}

		return buildAggregateField(toCondition, filteredAggregator, columnName);
	}

	protected SelectFieldOrAsterisk buildExpressionField(String columnName, FilteredAggregator filteredAggregator) {
		// Do not call `name` to make sure it is not qualified
		Field<?> unaliasedField = DSL.field(DSL.sql(columnName));

		if (!filteredAggregator.getFilter().isMatchAll()) {
			// BEWARE It is unclear how this could be managed: how to help TableQueryV3 producing valid FILTERs?
			throw new NotYetImplementedException(
					"FILTER with `ExpressionAggregation` is not managed. filteredAggregator=" + filteredAggregator);
		}

		return unaliasedField.as(filteredAggregator.getAlias());
	}

	protected SelectFieldOrAsterisk buildAggregateField(ISliceToJooqCondition toCondition,
			FilteredAggregator filteredAggregator,
			String columnName) {
		Name namedColumn = name(columnName);

		ConditionWithFilter condition = toCondition.toConditionSplitLeftover(filteredAggregator.getFilter());
		if (!condition.getLeftover().isMatchAll()) {
			log.debug("FILTER with a postFilter. filter={}",
					PepperLogHelper.getObjectAndClass(filteredAggregator.getFilter()));
		}

		Condition conditionInCase = buildConditionInCase(condition);

		Aggregator a = filteredAggregator.getAggregator();
		String aggregationKey = a.getAggregationKey();

		Field<Object> fieldWithoutCase = DSL.field(namedColumn);
		AggregateFunction<?> sqlAggFunction =
				buildAggregateFunction(aggregationKey, a, namedColumn, fieldWithoutCase, conditionInCase);

		Field<?> unaliasedField = applyFilterCondition(condition, sqlAggFunction);

		return unaliasedField.as(filteredAggregator.getAlias());
	}

	protected Condition buildConditionInCase(ConditionWithFilter condition) {
		boolean needCase = !(condition.getCondition() instanceof True) && !canFilterAggregates();
		if (needCase) {
			return condition.getCondition();
		}
		return DSL.trueCondition();
	}

	protected AggregateFunction<?> buildAggregateFunction(String aggregationKey,
			Aggregator a,
			Name namedColumn,
			Field<Object> fieldWithoutCase,
			Condition conditionInCase) {
		Field<Object> fieldToAggregate = asCase(conditionInCase, fieldWithoutCase);

		// TODO How not to define the output type from here (e.g. accept BigInteger or `double`, as would be
		// outputed by DuckDB)
		// https://stackoverflow.com/questions/79692856/jooq-dynamic-aggregated-types
		if (SumAggregation.KEY.equals(aggregationKey)) {
			return aggregate("sum", fieldToAggregate);
		} else if (MaxAggregation.KEY.equals(aggregationKey)) {
			return DSL.max(fieldToAggregate);
		} else if (MinAggregation.KEY.equals(aggregationKey)) {
			return DSL.min(fieldToAggregate);
		} else if (AvgAggregation.isAvg(aggregationKey)) {
			return aggregate("avg", fieldToAggregate);
		} else if (CountAggregation.isCount(aggregationKey)) {
			return buildCountAggregate(fieldWithoutCase, conditionInCase);
		} else if (RankAggregation.isRank(aggregationKey)) {
			return buildRankAggregate(a, fieldToAggregate);
		} else if (CoalesceAggregation.KEY.equals(aggregationKey)) {
			// `CoalesceAggregation` ("the column is constant for the slice — return any one value") maps to
			// `any_value(col)` (DuckDB / standard SQL since 2023): same row-preserving guarantee, no
			// double-counting risk. The DRILLTHROUGH path no longer relies on this (it bypasses GROUP BY via
			// `streamRawRows`), but the mapping stays valid for any caller emitting Coalesce in a regular query.
			return aggregate("any_value", fieldToAggregate);
		} else {
			return onCustomAggregation(a, namedColumn, conditionInCase);
		}
	}

	protected AggregateFunction<?> buildCountAggregate(Field<Object> fieldWithoutCase, Condition conditionInCase) {
		if (fieldWithoutCase.equals(asCase(conditionInCase, fieldWithoutCase))
		// || !DSL.name("*").equals(fieldWithoutCase)
		) {
			// No case/filter
			return DSL.count(fieldWithoutCase);
		}

		// Case: rewrap ensuring this is wrapped with `COUNT(CASE ... THEN 1)`
		Field<?> fieldAs1 = DSL.field(DSL.value(1));
		Field<?> caseOnFieldAs1 = asCase(conditionInCase, fieldAs1);
		return DSL.count(caseOnFieldAs1);
	}

	protected AggregateFunction<?> buildRankAggregate(Aggregator a, Field<Object> fieldToAggregate) {
		RankAggregation agg = (RankAggregation) operatorFactory.makeAggregation(a);
		String duckDbFunction;

		if (agg.isAscElseDesc()) {
			duckDbFunction = "arg_min";
		} else {
			duckDbFunction = "arg_max";
		}

		// https://duckdb.org/docs/stable/sql/functions/aggregates.html#arg_maxarg-val-n
		Name functionName = DSL.systemName(duckDbFunction);
		Param<Integer> rank = DSL.val(agg.getRank());
		return DSL.aggregate(functionName, Object.class, fieldToAggregate, fieldToAggregate, rank);
	}

	protected Field<?> applyFilterCondition(ConditionWithFilter condition, AggregateFunction<?> sqlAggFunction) {
		if (condition.getCondition() instanceof True) {
			return sqlAggFunction;
		}

		boolean needCase = !(condition.getCondition() instanceof True) && !canFilterAggregates();
		if (needCase) {
			return sqlAggFunction;
		}

		return sqlAggFunction.filterWhere(condition.getCondition());
	}

	// https://stackoverflow.com/questions/79692856/jooq-dynamic-aggregated-types
	protected AggregateFunction<Object> aggregate(String aggregationFunction, Field<Object> field) {
		return DSL.aggregate(DSL.systemName(aggregationFunction), field.getDataType(), field);
	}

	protected <T> Field<T> asCase(Condition condition, Field<T> field) {
		if (condition instanceof True) {
			return field;
		} else {
			return DSL.when(condition, field);
		}
	}

	/**
	 * 
	 * @param aggregator
	 * @param namedColumn
	 * @param condition
	 *            a condition to be applied. May be a `TrueCondition` if there is no {@link Condition} to apply.
	 * @return
	 */
	protected AggregateFunction<?> onCustomAggregation(Aggregator aggregator, Name namedColumn, Condition condition) {
		Field<Object> fieldWithoutCase = DSL.field(namedColumn);
		Field<Object> fieldToAggregate = asCase(condition, fieldWithoutCase);

		return onCustomAggregation(aggregator, fieldToAggregate);
	}

	protected AggregateFunction<?> onCustomAggregation(Aggregator aggregator, Field<Object> fieldToAggregate) {
		String aggregationKey = aggregator.getAggregationKey();

		// TODO Could we prefer some generic aggregation? (e.g. `array_agg` in DuckDB)
		throw new UnsupportedOperationException("SQL does not support aggregationKey=%s".formatted(aggregationKey));
	}

	@Deprecated(since = "TODO Migrate unitTests")
	public QueryWithLeftover prepareSliceQuery(TableQuery tableQuery) {
		return prepareSliceQuery(TableQueryV3.edit(tableQuery).build());
	}

	public static String groupingAlias(String c) {
		return "grouping_" + c.replaceAll("[\".]", "") + "_";
	}

}
