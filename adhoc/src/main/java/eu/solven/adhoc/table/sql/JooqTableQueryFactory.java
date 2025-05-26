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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.jooq.AggregateFunction;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.GroupField;
import org.jooq.Name;
import org.jooq.OrderField;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.SelectConnectByStep;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.SelectHavingStep;
import org.jooq.SelectJoinStep;
import org.jooq.SortField;
import org.jooq.TableLike;
import org.jooq.True;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultDataType;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.RankAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.operator.IOperatorsFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorsFactory;
import eu.solven.adhoc.measure.sum.AvgAggregation;
import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.sum.ExpressionAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.INotFilter;
import eu.solven.adhoc.query.filter.IOrFilter;
import eu.solven.adhoc.query.filter.NotFilter;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import eu.solven.adhoc.query.filter.value.StringMatcher;
import eu.solven.adhoc.query.groupby.IHasSqlExpression;
import eu.solven.adhoc.query.table.FilteredAggregator;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.query.top.AdhocTopClause;
import eu.solven.adhoc.table.transcoder.ITableTranscoder;
import eu.solven.adhoc.table.transcoder.TranscodingContext;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * This is especially important to make sure all calls to {@link ITableTranscoder} relies on a
 * {@link TranscodingContext}
 *
 * @author Benoit Lacelle
 */
// @RequiredArgsConstructor
@Builder
@Slf4j
public class JooqTableQueryFactory implements IJooqTableQueryFactory {
	@NonNull
	@Builder.Default
	IOperatorsFactory operatorsFactory = new StandardOperatorsFactory();

	@NonNull
	final TableLike<?> table;

	@NonNull
	DSLContext dslContext;

	// Typically used for RedShift when it is queried with PostgreSQL dialect
	@Default
	boolean canGroupByAll = false;

	/**
	 * Holds a Set of SQL {@link Condition}s, given an {@link IAdhocFilter}. Some filters may not be convertible into
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
		IAdhocFilter postFilter = IAdhocFilter.MATCH_ALL;
	}

	@Override
	public QueryWithLeftover prepareQuery(TableQueryV2 tableQuery) {
		ConditionWithFilter conditionAndLeftover = toConditions(tableQuery);
		AggregatedRecordFields fields = makeSelectedColumns(tableQuery, conditionAndLeftover.getPostFilter());

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
		Collection<GroupField> groupFields = makeGroupingFields(tableQuery, conditionAndLeftover.getPostFilter());
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
				.leftover(conditionAndLeftover.getPostFilter())
				.fields(fields)
				.build();
	}

	protected ConditionWithFilter toConditions(TableQueryV2 tableQuery) {
		Collection<Condition> conditions = new ArrayList<>();
		Collection<IAdhocFilter> leftoverFilters = new ArrayList<>();

		// Conditions from filters
		{
			IAdhocFilter filter = tableQuery.getFilter();
			ConditionWithFilter conditionWithFilter = toCondition(filter);

			conditions.add(conditionWithFilter.getCondition());
			leftoverFilters.add(conditionWithFilter.getPostFilter());
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

		fields.getLateColumns().forEach(lateColumn -> {
			Field<Object> field = columnAsField(ReferencedColumn.ref(lateColumn));
			selectedFields.add(field);
		});

		if (selectedFields.isEmpty()) {
			// Typically happens on EmptyAggregation
			// We force one field to prevent JooQ querying automatically for `*`
			selectedFields.add(DSL.val(1));
		}

		return selectedFields;
	}

	// @Override
	protected AggregatedRecordFields makeSelectedColumns(TableQueryV2 tableQuery, IAdhocFilter leftover) {
		return TableQuery.makeSelectedColumns(tableQuery, leftover);
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

	protected boolean isExpression(String columnName) {
		return AdhocJooqHelper.isExpression(columnName);
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
	protected Collection<GroupField> makeGroupingFields(TableQueryV2 tableQuery, IAdhocFilter leftoverFilter) {
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

	private boolean canGroupByAll() {
		return canGroupByAll || dslContext.dialect() == SQLDialect.DUCKDB;
	}

	private List<? extends OrderField<?>> getOptionalOrders(TableQueryV2 tableQuery) {
		AdhocTopClause topClause = tableQuery.getTopClause();
		List<? extends OrderField<?>> columns = topClause.getColumns().stream().map(c -> {
			Field<Object> field = columnAsField(c);

			SortField<Object> desc;
			if (topClause.isDesc()) {
				desc = field.desc();
			} else {
				desc = field.asc();
			}

			return desc;
		}).toList();

		return columns;
	}

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

				AggregateFunction<?> sqlAggFunction;
				if (SumAggregation.KEY.equals(aggregationKey)) {
					// DSL.field(namedColumn, Number.class); fails with `Type class java.lang.Number is not supported in
					// dialect `DEFAULT`
					Field<Double> field =
							DSL.field(namedColumn, DefaultDataType.getDataType(dslContext.dialect(), double.class));

					sqlAggFunction = DSL.sum(field);
				} else if (MaxAggregation.KEY.equals(aggregationKey)) {
					Field<?> field = DSL.field(namedColumn);
					sqlAggFunction = DSL.max(field);
				} else if (MinAggregation.KEY.equals(aggregationKey)) {
					Field<?> field = DSL.field(namedColumn);
					sqlAggFunction = DSL.min(field);
				} else if (AvgAggregation.isAvg(aggregationKey)) {
					Field<Double> field =
							DSL.field(namedColumn, DefaultDataType.getDataType(dslContext.dialect(), double.class));
					sqlAggFunction = DSL.avg(field);
				} else if (CountAggregation.isCount(aggregationKey)) {
					Field<?> field = DSL.field(namedColumn);
					sqlAggFunction = DSL.count(field);
				} else if (RankAggregation.isRank(aggregationKey)) {
					RankAggregation agg = (RankAggregation) operatorsFactory.makeAggregation(a);

					Field<?> field = DSL.field(namedColumn);
					String duckDbFunction;

					if (agg.isAscElseDesc()) {
						duckDbFunction = "arg_min";
					} else {
						duckDbFunction = "arg_max";
					}

					// https://duckdb.org/docs/stable/sql/functions/aggregates.html#arg_maxarg-val-n
					sqlAggFunction = DSL
							.aggregate(duckDbFunction, Object.class, field, field, DSL.field(DSL.val(agg.getRank())));
				} else {
					throw new UnsupportedOperationException(
							"SQL does not support aggregationKey=%s".formatted(aggregationKey));
				}

				if (filteredAggregator.getFilter().isMatchAll()) {
					unaliasedField = sqlAggFunction;
				} else {
					ConditionWithFilter condition = toCondition(filteredAggregator.getFilter());

					if (!condition.getPostFilter().isMatchAll()) {
						throw new NotYetImplementedException("FILTER with a postFilter. filter="
								+ PepperLogHelper.getObjectAndClass(filteredAggregator.getFilter()));
					}

					unaliasedField = sqlAggFunction.filterWhere(condition.getCondition());
				}

				unaliasedField = sqlAggFunction;
			}

			return unaliasedField.as(filteredAggregator.getAlias());
		}
	}

	protected ConditionWithFilter toCondition(IAdhocFilter filter) {
		if (filter.isMatchAll()) {
			return ConditionWithFilter.builder().condition(DSL.trueCondition()).build();
		} else if (filter.isMatchNone()) {
			return ConditionWithFilter.builder().condition(DSL.falseCondition()).build();
		} else if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			Optional<Condition> optColumnFilterAsCondition = toCondition(columnFilter);
			if (optColumnFilterAsCondition.isEmpty()) {
				log.debug("{} will be applied manually", columnFilter);
				return ConditionWithFilter.builder().postFilter(columnFilter).build();
			} else {
				return ConditionWithFilter.builder().condition(optColumnFilterAsCondition.get()).build();
			}
		} else if (filter.isNot() && filter instanceof INotFilter notFilter) {
			ConditionWithFilter negated = toCondition(notFilter.getNegated());

			// `!(sqlCondition && postFilter) === !sqlCondition || !postFilter`
			// ConditionWithFilter can not express an OR, so we just ensure one of `sqlCondition` or `postFilter` is
			// `matchAll`.
			boolean oneIsMatchAll = false;

			IAdhocFilter negatedPostFilter;
			if (negated.getPostFilter().isMatchAll()) {
				negatedPostFilter = IAdhocFilter.MATCH_ALL;
				oneIsMatchAll = true;
			} else {
				// There is no postFilter: keep it as matchAll
				negatedPostFilter = NotFilter.not(negated.getPostFilter());
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

			return ConditionWithFilter.builder().postFilter(negatedPostFilter).condition(negatedCondition).build();
		} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			Set<IAdhocFilter> operands = andFilter.getOperands();
			// TODO Detect and report if multiple conditions hits the same column
			// It would be the symptom of conflicting transcoding
			List<ConditionWithFilter> conditions = operands.stream().map(this::toCondition).toList();

			List<Condition> sqlConditions = conditions.stream().map(ConditionWithFilter::getCondition).toList();
			List<IAdhocFilter> leftoversConditions =
					conditions.stream().map(ConditionWithFilter::getPostFilter).toList();

			return and(sqlConditions, leftoversConditions);
		} else if (filter.isOr() && filter instanceof IOrFilter orFilter) {
			Set<IAdhocFilter> operands = orFilter.getOperands();

			List<ConditionWithFilter> conditions = operands.stream().map(this::toCondition).toList();

			boolean anyPostFilter =
					conditions.stream().map(ConditionWithFilter::getPostFilter).anyMatch(f -> !f.isMatchAll());

			if (anyPostFilter) {
				log.debug("A postFilter with OR (`{}`) leads to no table filtering", filter);
				return ConditionWithFilter.builder().condition(DSL.trueCondition()).postFilter(filter).build();
			} else {
				List<Condition> sqlConditions = conditions.stream().map(ConditionWithFilter::getCondition).toList();

				// There is no postFilter: table will handle the filter
				return ConditionWithFilter.builder()
						.condition(DSL.or(sqlConditions))
						.postFilter(IAdhocFilter.MATCH_ALL)
						.build();
			}
		} else {
			throw new UnsupportedOperationException(
					"Not handled: %s".formatted(PepperLogHelper.getObjectAndClass(filter)));
		}
	}

	protected ConditionWithFilter and(Collection<Condition> sqlConditions,
			Collection<IAdhocFilter> leftoversConditions) {
		return ConditionWithFilter.builder()
				.condition(andSql(sqlConditions))
				.postFilter(AndFilter.and(leftoversConditions))
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
	 * @return
	 */
	protected Optional<Condition> toCondition(IColumnFilter columnFilter) {
		IValueMatcher valueMatcher = columnFilter.getValueMatcher();
		String column = columnFilter.getColumn();

		Condition condition;
		final Field<Object> field = DSL.field(name(column));
		switch (valueMatcher) {
		case NullMatcher nullMatcher -> condition = DSL.condition(field.isNull());
		case InMatcher inMatcher -> {
			Set<?> operands = inMatcher.getOperands();

			if (operands.stream().anyMatch(o -> o instanceof IValueMatcher)) {
				// Please fill a ticket, various such cases could be handled
				throw new UnsupportedOperationException("There is a IValueMatcher amongst " + operands);
			}

			condition = DSL.condition(field.in(operands));
		}
		case EqualsMatcher equalsMatcher -> condition = DSL.condition(field.eq(equalsMatcher.getOperand()));
		case LikeMatcher likeMatcher -> condition = DSL.condition(field.like(likeMatcher.getLike()));
		case StringMatcher stringMatcher -> condition =
				DSL.condition(field.cast(String.class).eq(stringMatcher.getString()));
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
			condition = DSL.condition(jooqCondition);
		}
		default -> condition = null;
		// throw new UnsupportedOperationException(
		// "Not handled: %s".formatted(PepperLogHelper.getObjectAndClass(columnFilter)));
		}

		return Optional.ofNullable(condition);
	}

	@Deprecated(since = "TODO Migrate unitTests")
	public QueryWithLeftover prepareQuery(TableQuery tableQuery) {
		return prepareQuery(TableQueryV2.fromV1(tableQuery));
	}

}
