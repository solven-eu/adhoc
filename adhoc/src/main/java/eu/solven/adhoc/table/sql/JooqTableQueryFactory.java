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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.IHasOperands;
import eu.solven.adhoc.query.filter.INotFilter;
import eu.solven.adhoc.query.filter.IOrFilter;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.value.AndMatcher;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NotMatcher;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import eu.solven.adhoc.query.filter.value.OrMatcher;
import eu.solven.adhoc.query.filter.value.StringMatcher;
import eu.solven.adhoc.query.groupby.IHasSqlExpression;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.query.top.AdhocTopClause;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.transcoder.AliasingContext;
import eu.solven.adhoc.table.transcoder.ITableAliaser;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * This is especially important to make sure all calls to {@link ITableAliaser} relies on a {@link AliasingContext}
 *
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
@SuppressWarnings({ "PMD.GodClass", "PMD.CouplingBetweenObjects" })
public class JooqTableQueryFactory implements IJooqTableQueryFactory {
	@NonNull
	@Builder.Default
	IOperatorFactory operatorFactory = StandardOperatorFactory.builder().build();

	@NonNull
	final TableLike<?> table;

	@NonNull
	DSLContext dslContext;

	@Getter(AccessLevel.PACKAGE)
	JooqTableCapabilities capabilities;

	public JooqTableQueryFactory(IOperatorFactory operatorFactory, TableLike<?> table, DSLContext dslContext) {
		this(operatorFactory, table, dslContext, JooqTableCapabilities.from(dslContext.dialect()));
	}

	public JooqTableQueryFactory(IOperatorFactory operatorFactory,
			TableLike<?> table,
			DSLContext dslContext,
			JooqTableCapabilities capabilities) {
		this.operatorFactory = operatorFactory;
		this.table = table;
		this.dslContext = dslContext;
		if (capabilities == null) {
			this.capabilities = JooqTableCapabilities.from(dslContext.dialect());
		} else {
			this.capabilities = capabilities;
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

	@Override
	public QueryWithLeftover prepareQuery(TableQueryV2 tableQuery) {
		ConditionWithFilter conditionAndLeftover = toConditions(tableQuery);

		// Leftover in FILTER clause
		Map<String, ISliceFilter> aggregateToLeftover = new LinkedHashMap<>();

		{
			tableQuery.getAggregators().forEach(filtered -> {
				ISliceFilter aggregatorFilter = filtered.getFilter();
				ConditionWithFilter conditionWithFilter = toConditionSplitLeftover(aggregatorFilter);

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
		Collection<SelectFieldOrAsterisk> selectedFields = makeSelectedFields(tableQuery, fields);

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
				.query(resultQuery)
				.leftover(conditionAndLeftover.getLeftover())
				.aggregatorToLeftovers(aggregateToLeftover)
				.fields(fields)
				.build();
	}

	protected ConditionWithFilter toConditions(TableQueryV2 tableQuery) {
		Collection<Condition> conditions = new ArrayList<>();
		Collection<ISliceFilter> leftoverFilters = new ArrayList<>();

		// Conditions from filters
		{
			ISliceFilter filter = tableQuery.getFilter();
			ConditionWithFilter conditionWithFilter = toConditionSplitLeftover(filter);

			conditions.add(conditionWithFilter.getCondition());
			leftoverFilters.add(conditionWithFilter.getLeftover());
		}

		// AND conditions from measures and from filters
		return and(conditions, leftoverFilters);
	}

	protected List<SelectFieldOrAsterisk> makeSelectedFields(TableQueryV2 tableQuery, AggregatedRecordFields fields) {
		List<SelectFieldOrAsterisk> selectedFields = new ArrayList<>();
		tableQuery.getAggregators().stream().distinct().map(a -> {
			try {
				return toSqlAggregatedColumn(a);
			} catch (RuntimeException e) {
				throw new IllegalArgumentException("Issue converting to SQL: %s".formatted(a), e);
			}
		})
				// EmptyAggregation leads to no SQL aggregation
				.filter(Objects::nonNull)
				.forEach(selectedFields::add);

		tableQuery.getGroupBy().getNameToColumn().values().forEach(column -> {
			Field<Object> field = columnAsField(column);
			selectedFields.add(field);
		});

		fields.getLeftovers().forEach(leftover -> {
			Field<Object> field = columnAsField(ReferencedColumn.ref(leftover));
			selectedFields.add(field);
		});

		if (selectedFields.isEmpty()) {
			// Typically happens on EmptyAggregation
			// We force one field to prevent JooQ querying automatically for `*`
			// BEWARE Rely on `count(1)` and not `1`, else DuckDB considers all fields are requested, and the groupBy
			// lists all rows.
			selectedFields.add(DSL.aggregate("count", long.class, DSL.val(1)));
		}

		return selectedFields;
	}

	// @Override
	protected AggregatedRecordFields makeSelectedColumns(TableQueryV2 tableQuery, Set<ISliceFilter> leftovers) {
		return TableQuery.makeSelectedColumns(tableQuery, leftovers);
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
			// If we were to have some aliasing around here, aliases should probably not be appled on groupBy
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
	protected Collection<GroupField> makeGroupingFields(TableQueryV2 tableQuery, ISliceFilter leftoverFilter) {
		List<GroupField> groupedFields = new ArrayList<>();
		if (canGroupByAll()) {
			// `GROUP BY ALL` is supported by: DuckDB, RedShift, More?
			// https://duckdb.org/docs/stable/sql/query_syntax/groupby.html#group-by-all
			// https://docs.aws.amazon.com/redshift/latest/dg/r_GROUP_BY_clause.html
			groupedFields.add(DSL.field(DSL.unquotedName("ALL")));
		} else {
			tableQuery.getGroupBy().getNameToColumn().values().forEach(column -> {
				Field<Object> field = columnAsField(column);
				groupedFields.add(field);
			});

			FilterHelpers.getFilteredColumns(leftoverFilter).forEach(column -> {
				Field<Object> field = columnAsField(ReferencedColumn.ref(column));
				groupedFields.add(field);
			});
		}

		return groupedFields;
	}

	protected boolean canGroupByAll() {
		return capabilities.isAbleToGroupByAll();
	}

	protected boolean canFilterAggregates() {
		return capabilities.isAbleToFilterAggregates();
	}

	protected List<? extends OrderField<?>> getOptionalOrders(TableQueryV2 tableQuery) {
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
	protected SelectFieldOrAsterisk toSqlAggregatedColumn(FilteredAggregator filteredAggregator) {
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
					// BEWARE It is unclear how this could be managed: how to help TableQueryV2 producing valid FILTERs?
					throw new NotYetImplementedException(
							"FILTER with `ExpressionAggregation` is not managed. filteredAggregator="
									+ filteredAggregator);
				}
			} else {
				Name namedColumn = name(columnName);

				ConditionWithFilter condition = toConditionSplitLeftover(filteredAggregator.getFilter());
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

	protected ConditionWithFilter toConditionSplitLeftover(ISliceFilter filter) {
		// Split `AND` to enable `preFilter AND postFilter`
		// This will also cover `NOT(OR(...))`
		Set<ISliceFilter> ands = FilterHelpers.splitAnd(filter);

		// Partition conditions which can be translated into SQL or not.
		// BEWARE It will lead to translating twice to SQL
		Map<Boolean, List<ISliceFilter>> conditionAndFilters =
				ands.stream().collect(Collectors.partitioningBy(f -> this.toCondition(f).getLeftover().isMatchAll()));

		ISliceFilter withoutPostFilter = FilterBuilder.and(conditionAndFilters.get(true)).optimize();
		ISliceFilter withPostFilter = FilterBuilder.and(conditionAndFilters.get(false)).optimize();

		if (ISliceFilter.MATCH_ALL.equals(withPostFilter)) {
			// There is no customCondition: restore the original condition as it may have be changed by the
			// `partitioninBy` and the `optimize`
			withoutPostFilter = filter;
		}

		ConditionWithFilter conditionWithout = toCondition(withoutPostFilter);
		if (!ISliceFilter.MATCH_ALL.equals(conditionWithout.getLeftover())) {
			throw new IllegalStateException("Expected no postFilter from %s".formatted(withoutPostFilter));
		}

		return ConditionWithFilter.builder()
				.condition(conditionWithout.getCondition())
				.leftover(withPostFilter)
				.build();
	}

	protected ConditionWithFilter toCondition(ISliceFilter filter) {
		return toCondition(filter, false);
	}

	/**
	 * 
	 * @param filter
	 * @param hasParentNot
	 *            indicates if this expression is wrapped into a `NOT` which requires specific `NULL` management (e.g.
	 *            `NOT(c = 'v')` is false if c is `NULL`).
	 * @return
	 */
	@SuppressWarnings("PMD.CognitiveComplexity")
	protected ConditionWithFilter toCondition(ISliceFilter filter, boolean hasParentNot) {
		if (filter.isMatchAll()) {
			return ConditionWithFilter.builder().condition(DSL.trueCondition()).build();
		} else if (filter.isMatchNone()) {
			return ConditionWithFilter.builder().condition(DSL.falseCondition()).build();
		} else if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			Optional<Condition> optColumnFilterAsCondition = toCondition(columnFilter, hasParentNot);
			if (optColumnFilterAsCondition.isEmpty()) {
				log.debug("{} will be applied manually", columnFilter);
				return ConditionWithFilter.builder().leftover(columnFilter).build();
			} else {
				return ConditionWithFilter.builder().condition(optColumnFilterAsCondition.get()).build();
			}
		} else if (filter.isNot() && filter instanceof INotFilter notFilter) {
			ConditionWithFilter negated = toCondition(notFilter.getNegated(), true);

			// ConditionWithFilter can not express an OR, so we just ensure one of `sqlCondition` or `postFilter` is
			// `matchAll`.
			boolean oneIsMatchAll = false;

			ISliceFilter negatedPostFilter;
			if (negated.getLeftover().isMatchAll()) {
				// There is no leftover: keep it as matchAll
				negatedPostFilter = ISliceFilter.MATCH_ALL;
				oneIsMatchAll = true;
			} else {
				negatedPostFilter = negated.getLeftover().negate();
			}

			Condition negatedCondition;
			if (negated.getCondition() instanceof True) {
				negatedCondition = DSL.trueCondition();
				oneIsMatchAll = true;
			} else {
				negatedCondition = negated.getCondition().not();
			}

			if (!oneIsMatchAll) {
				throw new NotYetImplementedException("Converting `%s` to SQL".formatted(filter));
			}

			return ConditionWithFilter.builder().leftover(negatedPostFilter).condition(negatedCondition).build();
		} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			Set<ISliceFilter> operands = andFilter.getOperands();
			// TODO Detect and report if multiple conditions hits the same column
			// It would be the symptom of conflicting transcoding
			List<ConditionWithFilter> conditions = operands.stream().map(c -> toCondition(c, hasParentNot)).toList();

			List<Condition> sqlConditions = conditions.stream().map(ConditionWithFilter::getCondition).toList();
			List<ISliceFilter> leftoversConditions = conditions.stream().map(ConditionWithFilter::getLeftover).toList();

			return and(sqlConditions, leftoversConditions);
		} else if (filter.isOr() && filter instanceof IOrFilter orFilter) {
			Set<ISliceFilter> operands = orFilter.getOperands();

			List<ConditionWithFilter> conditions = operands.stream().map(c -> toCondition(c, hasParentNot)).toList();

			boolean anyPostFilter =
					conditions.stream().map(ConditionWithFilter::getLeftover).anyMatch(f -> !f.isMatchAll());

			if (anyPostFilter) {
				log.debug("A postFilter with OR (`{}`) leads to no table filtering", filter);
				return ConditionWithFilter.builder().condition(DSL.trueCondition()).leftover(filter).build();
			} else {
				List<Condition> sqlConditions = conditions.stream().map(ConditionWithFilter::getCondition).toList();

				// There is no postFilter: table will handle the filter
				return ConditionWithFilter.builder()
						.condition(DSL.or(sqlConditions))
						.leftover(ISliceFilter.MATCH_ALL)
						.build();
			}
		} else {
			throw new UnsupportedOperationException(
					"Not handled: %s".formatted(PepperLogHelper.getObjectAndClass(filter)));
		}
	}

	protected ConditionWithFilter and(Collection<Condition> sqlConditions,
			Collection<ISliceFilter> leftoversConditions) {
		return ConditionWithFilter.builder()
				.condition(andSql(sqlConditions))
				.leftover(FilterBuilder.and(leftoversConditions).optimize())
				.build();
	}

	protected Condition andSql(Collection<Condition> sqlConditions) {
		List<Condition> notTrueConditions = sqlConditions.stream()
				// Typically happens on `COUNT(*)`
				.filter(c -> !(c instanceof True))
				.toList();

		if (notTrueConditions.isEmpty()) {
			return DSL.trueCondition();
		}

		return DSL.and(notTrueConditions);
	}

	/**
	 *
	 * @param columnFilter
	 * @return if empty, it means the {@link IColumnFilter} can not be transcoded into a {@link Condition}.
	 */
	protected Optional<Condition> toCondition(IColumnFilter columnFilter) {
		return toCondition(columnFilter, false);
	}

	@SuppressWarnings("PMD.AssignmentInOperand")
	protected Optional<Condition> toCondition(IColumnFilter columnFilter, boolean hasParentNot) {
		IValueMatcher valueMatcher = columnFilter.getValueMatcher();
		String column = columnFilter.getColumn();

		final Field<Object> field = DSL.field(name(column));

		Condition condition;
		switch (valueMatcher) {
		case NullMatcher nullMatcher -> condition = DSL.condition(field.isNull());
		case InMatcher inMatcher -> {
			Set<?> operands = inMatcher.getOperands();

			if (operands.stream().anyMatch(o -> o instanceof IValueMatcher)) {
				// Please fill a ticket, various such cases could be handled
				throw new UnsupportedOperationException("There is a IValueMatcher amongst " + operands);
			}

			condition = wrap(hasParentNot, field, field.in(operands));
		}
		case EqualsMatcher equalsMatcher -> condition = wrap(hasParentNot, field, field.eq(equalsMatcher.getOperand()));
		case LikeMatcher likeMatcher -> condition = wrap(hasParentNot, field, field.like(likeMatcher.getPattern()));
		case StringMatcher stringMatcher -> condition =
				wrap(hasParentNot, field, field.cast(String.class).eq(stringMatcher.getString()));

		case ComparingMatcher comparingMatcher -> {
			Object operand = comparingMatcher.getOperand();

			Condition jooqCondition;
			if (comparingMatcher.isGreaterThan()) {
				if (comparingMatcher.isMatchIfEqual()) {
					jooqCondition = field.greaterOrEqual(operand);
				} else {
					jooqCondition = field.greaterThan(operand);
				}
			} else {
				if (comparingMatcher.isMatchIfEqual()) {
					jooqCondition = field.lessOrEqual(operand);
				} else {
					jooqCondition = field.lessThan(operand);
				}
			}
			condition = wrap(hasParentNot, field, jooqCondition);
		}

		case AndMatcher andMatcher -> {
			List<Optional<Condition>> optConditions = toConditions(column, andMatcher, hasParentNot);

			if (optConditions.stream().anyMatch(Optional::isEmpty)) {
				return Optional.empty();
			}

			condition = DSL.and(optConditions.stream().map(Optional::get).toList());
		}
		case OrMatcher orMatcher -> {
			List<Optional<Condition>> optConditions = toConditions(column, orMatcher, hasParentNot);

			if (optConditions.stream().anyMatch(Optional::isEmpty)) {
				return Optional.empty();
			}

			condition = DSL.or(optConditions.stream().map(Optional::get).toList());
		}
		case NotMatcher notMatcher -> {
			Optional<Condition> optConditions =
					toCondition(ColumnFilter.builder().column(column).valueMatcher(notMatcher.getNegated()).build(),
							true);

			if (optConditions.isEmpty()) {
				return Optional.empty();
			}

			condition = DSL.not(optConditions.get());
		}
		default -> condition = onCustomCondition(column, valueMatcher, hasParentNot);
		}

		return Optional.ofNullable(condition);
	}

	protected Condition wrap(boolean hasParentNot, Field<Object> field, Condition condition) {
		if (hasParentNot) {
			// https://www.w3schools.com/sql/sql_not.asp
			// https://dba.stackexchange.com/questions/333948/not-equal-to-operator-is-not-returning-null-values-in-sql-server
			// https://stackoverflow.com/questions/5658457/not-equal-operator-on-null

			// Typically `not(c = 'v')` returns false if `c IS NULL` while `not(c IS NOT NULL AND c = 'v')` returns true
			return DSL.and(field.isNotNull(), condition);
		} else {
			return DSL.condition(condition);
		}
	}

	protected List<Optional<Condition>> toConditions(String column,
			IHasOperands<IValueMatcher> hasOperands,
			boolean hasParentNot) {
		return hasOperands.getOperands().stream().map(subValueMatcher -> {
			ColumnFilter subFilter = ColumnFilter.builder().column(column).valueMatcher(subValueMatcher).build();
			return toCondition(subFilter, hasParentNot);
		}).toList();
	}

	/**
	 * 
	 * @param column
	 * @param valueMatcher
	 * @param hasParentNot
	 * @return By default, we return null so that this {@link IValueMatcher} is managed as post-filtering by Adhoc, over
	 *         the {@link ITableWrapper} result.
	 */
	protected Condition onCustomCondition(String column, IValueMatcher valueMatcher, boolean hasParentNot) {
		// throw new UnsupportedOperationException(
		// "Not handled: %s matches %s".formatted(column, PepperLogHelper.getObjectAndClass(valueMatcher)));
		return null;
	}

	@Deprecated(since = "TODO Migrate unitTests")
	public QueryWithLeftover prepareQuery(TableQuery tableQuery) {
		return prepareQuery(TableQueryV2.fromV1(tableQuery));
	}

}
