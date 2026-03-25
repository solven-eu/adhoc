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
import eu.solven.adhoc.engine.tabular.optimizer.TableQueryFactory;
import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.measure.sum.AvgAggregation;
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
	public static final String PREFIX_GROUPING = "adhoc_grouping_";

	@NonNull
	@Builder.Default
	final IOperatorFactory operatorFactory = StandardOperatorFactory.builder().build();

	@NonNull
	final TableLike<?> table;

	@NonNull
	final DSLContext dslContext;

	@Getter(AccessLevel.PACKAGE)
	final JooqTableCapabilities capabilities;

	@NonNull
	@Builder.Default
	final SliceToJooqConditionFactory sliceToCondition = new SliceToJooqConditionFactory();

	@NonNull
	@Default
	final IQueryPartitionor queryPartitionor = IQueryPartitionor.SINGLE_PARTITION;

	// BEWARE This is Delombokized. It customizes the case `capabilities == null`
	protected JooqTableQueryFactory(JooqTableQueryFactoryBuilder<?, ?> b) {
		if (b.operatorFactory$set) {
			this.operatorFactory = b.operatorFactory$value;
		} else {
			this.operatorFactory = $default$operatorFactory();
		}
		this.table = b.table;
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
	public QueryWithLeftover prepareQuery(TableQueryV2 tableQuery) {
		return prepareQuery(TableQueryV3.edit(tableQuery).build());
	}

	@Override
	public QueryWithLeftover prepareQuery(TableQueryV4 tableQuery) {
		TableQueryV3 v3 = tableQuery.asCoveringV3();

		if (tableQuery.isDebugOrExplain()) {
			long nbEvaluatedTableInducers = TableQueryV3.nbCuboids(v3);
			long tableStepsCount = tableQuery.streamV3().mapToLong(TableQueryV3::nbCuboids).sum();

			// prints percent with 1 digit.
			String percentEfficiency = TableQueryFactory.asPercent(tableStepsCount, nbEvaluatedTableInducers);
			log.info("[EXPLAIN] {} inducers evaluated by {} tableQuery (evaluating {} steps). Efficiency={} v3={}",
					tableStepsCount,
					1,
					nbEvaluatedTableInducers,
					percentEfficiency,
					v3);
		}
		return prepareQuery(v3);
	}

	protected QueryWithLeftover prepareQuery(TableQueryV3 tableQuery) {
		ISliceToJooqCondition toCondition = makeToCondition();

		ConditionWithFilter conditionAndLeftover = toConditions(toCondition, tableQuery);

		// Leftover in FILTER clause
		Map<String, ISliceFilter> aggregateToLeftover = new LinkedHashMap<>();

		{
			tableQuery.getAggregators().forEach(filtered -> {
				ISliceFilter aggregatorFilter = filtered.getFilter();
				ConditionWithFilter conditionWithFilter = toCondition.toConditionSplitLeftover(aggregatorFilter);

				if (!conditionWithFilter.getLeftover().isMatchAll()) {
					aggregateToLeftover.put(filtered.getAlias(), conditionWithFilter.getLeftover());
				}
			});
		}

		ImmutableSet<ISliceFilter> leftovers = ImmutableSet.<ISliceFilter>builder()
				.add(conditionAndLeftover.getLeftover())
				.addAll(aggregateToLeftover.values())
				.build();
		AggregatedRecordFields fields = makeSelectedColumns(tableQuery, leftovers);

		// `SELECT ...`
		Collection<SelectFieldOrAsterisk> selectedFields = makeSelectedFields(toCondition, tableQuery, fields);

		// `FROM ...`
		SelectJoinStep<Record> selectFrom = dslContext.select(selectedFields).from(table);

		// `WHERE ...`
		SelectConnectByStep<Record> selectFromWhere;
		if (conditionAndLeftover.getCondition() instanceof True) {
			selectFromWhere = selectFrom;
		} else {
			selectFromWhere = selectFrom.where(conditionAndLeftover.getCondition());
		}

		// `GROUP BY ...`
		Collection<GroupField> groupFields = makeGroupingFields(tableQuery, conditionAndLeftover.getLeftover());
		SelectHavingStep<Record> selectFromWhereGroupBy = selectFromWhere.groupBy(groupFields);

		// `ORDER BY ...`
		ResultQuery<Record> resultQuery;
		if (tableQuery.getTopClause().isPresent()) {
			Collection<? extends OrderField<?>> optOrderFields = getOptionalOrders(tableQuery);

			resultQuery = selectFromWhereGroupBy.orderBy(optOrderFields).limit(tableQuery.getTopClause().getLimit());
		} else {
			resultQuery = selectFromWhereGroupBy;
		}

		return QueryWithLeftover.builder()
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
				.queries(partitionQuery(resultQuery))
				.leftover(conditionAndLeftover.getLeftover())
				.aggregatorToLeftovers(aggregateToLeftover)
				.fields(fields)
				// .groupingColumns(groupingColumns)
				.build();
	}

	protected ISliceToJooqCondition makeToCondition() {
		return sliceToCondition.with(this::name);
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

	protected List<SelectFieldOrAsterisk> makeSelectedFields(ISliceToJooqCondition toCondition,
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

	protected AggregatedRecordFields makeSelectedColumns(TableQueryV3 tableQuery, Set<ISliceFilter> leftovers) {
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

	@SuppressWarnings("PMD.CognitiveComplexity")
	protected SelectFieldOrAsterisk toSqlAggregatedColumn(ISliceToJooqCondition toCondition,
			FilteredAggregator filteredAggregator) {
		Aggregator a = filteredAggregator.getAggregator();

		String aggregationKey = a.getAggregationKey();
		if (EmptyAggregation.isEmpty(aggregationKey)) {
			// There is no aggregation for empty: we just want to fetch groupBys
			return null;
		} else {
			String columnName = a.getColumnName();

			Field<?> unaliasedField;
			if (ExpressionAggregation.isExpression(aggregationKey)) {
				// Do not call `name` to make sure it is not qualified
				unaliasedField = DSL.field(DSL.sql(columnName));

				if (!filteredAggregator.getFilter().isMatchAll()) {
					// BEWARE It is unclear how this could be managed: how to help TableQueryV3 producing valid FILTERs?
					throw new NotYetImplementedException(
							"FILTER with `ExpressionAggregation` is not managed. filteredAggregator="
									+ filteredAggregator);
				}
			} else {
				Name namedColumn = name(columnName);

				ConditionWithFilter condition = toCondition.toConditionSplitLeftover(filteredAggregator.getFilter());
				if (!condition.getLeftover().isMatchAll()) {
					log.debug("FILTER with a postFilter. filter={}",
							PepperLogHelper.getObjectAndClass(filteredAggregator.getFilter()));
				}

				boolean needCase = !(condition.getCondition() instanceof True) && !canFilterAggregates();
				Condition conditionInCase;
				if (needCase) {
					conditionInCase = condition.getCondition();
				} else {
					conditionInCase = DSL.trueCondition();
				}

				AggregateFunction<?> sqlAggFunction;

				Field<Object> fieldWithoutCase = DSL.field(namedColumn);
				Field<Object> fieldToAggregate = asCase(conditionInCase, fieldWithoutCase);

				// TODO How not to define the output type from here (e.g. accept BigInteger or `double`, as would be
				// outputed by DuckDB)
				// https://stackoverflow.com/questions/79692856/jooq-dynamic-aggregated-types
				if (SumAggregation.KEY.equals(aggregationKey)) {
					sqlAggFunction = aggregate("sum", fieldToAggregate);
				} else if (MaxAggregation.KEY.equals(aggregationKey)) {
					sqlAggFunction = DSL.max(fieldToAggregate);
				} else if (MinAggregation.KEY.equals(aggregationKey)) {
					sqlAggFunction = DSL.min(fieldToAggregate);
				} else if (AvgAggregation.isAvg(aggregationKey)) {
					sqlAggFunction = aggregate("avg", fieldToAggregate);
				} else if (CountAggregation.isCount(aggregationKey)) {
					if (fieldWithoutCase.equals(fieldToAggregate) || !DSL.name("*").equals(namedColumn)) {
						// No case/filter
						sqlAggFunction = DSL.count(fieldWithoutCase);
					} else {
						// Case: rewrap ensuring this is wrapped with `COUNT(CASE ... THEN 1)`
						Field<?> fieldAs1 = DSL.field(DSL.value(1));
						Field<?> caseOnFieldAs1 = asCase(conditionInCase, fieldAs1);
						sqlAggFunction = DSL.count(caseOnFieldAs1);
					}
				} else if (RankAggregation.isRank(aggregationKey)) {
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
					sqlAggFunction =
							DSL.aggregate(functionName, Object.class, fieldToAggregate, fieldToAggregate, rank);
				} else {
					sqlAggFunction = onCustomAggregation(a, namedColumn, conditionInCase);
				}

				if (condition.getCondition() instanceof True) {
					unaliasedField = sqlAggFunction;
				} else {
					if (needCase) {
						// FILTER is already applied through a `CASE` as aggregated expression
						unaliasedField = sqlAggFunction;
					} else {
						unaliasedField = sqlAggFunction.filterWhere(condition.getCondition());
					}
				}
			}

			return unaliasedField.as(filteredAggregator.getAlias());
		}
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
	public QueryWithLeftover prepareQuery(TableQuery tableQuery) {
		return prepareQuery(TableQueryV3.edit(tableQuery).build());
	}

	public static String groupingAlias(String c) {
		return "grouping_" + c.replaceAll("[\".]", "") + "_";
	}

}
