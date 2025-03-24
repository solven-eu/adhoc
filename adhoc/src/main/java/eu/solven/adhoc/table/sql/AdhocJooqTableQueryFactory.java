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
import java.util.Set;
import java.util.stream.Collectors;

import org.jooq.AggregateFunction;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.GroupField;
import org.jooq.Name;
import org.jooq.OrderField;
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
import org.jooq.impl.DefaultDataType;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.aggregation.comparable.MinAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.CountAggregation;
import eu.solven.adhoc.measure.sum.EmptyAggregation;
import eu.solven.adhoc.measure.sum.ExpressionAggregation;
import eu.solven.adhoc.measure.sum.SumAggregation;
import eu.solven.adhoc.query.ICountMeasuresConstants;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.IOrFilter;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;
import eu.solven.adhoc.query.filter.value.EqualsMatcher;
import eu.solven.adhoc.query.filter.value.IValueMatcher;
import eu.solven.adhoc.query.filter.value.InMatcher;
import eu.solven.adhoc.query.filter.value.LikeMatcher;
import eu.solven.adhoc.query.filter.value.NullMatcher;
import eu.solven.adhoc.query.groupby.IHasSqlExpression;
import eu.solven.adhoc.query.table.TableQuery;
import eu.solven.adhoc.query.top.AdhocTopClause;
import eu.solven.adhoc.table.transcoder.IAdhocTableTranscoder;
import eu.solven.adhoc.table.transcoder.TranscodingContext;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This is especially important to make sure all calls to {@link IAdhocTableTranscoder} relies on a
 * {@link TranscodingContext}
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Builder
@Slf4j
public class AdhocJooqTableQueryFactory implements IAdhocJooqTableQueryFactory {
	// BEWARE Should we enable table as SQL or TableLike?
	@NonNull
	final TableLike<?> table;

	@NonNull
	DSLContext dslContext;

	@Override
	public ResultQuery<Record> prepareQuery(TableQuery tableQuery) {
		// `SELECT ...`
		Collection<SelectFieldOrAsterisk> selectedFields = makeSelectedFields(tableQuery);

		// `FROM ...`
		SelectJoinStep<Record> selectFrom = dslContext.select(selectedFields).from(table);

		// `WHERE ...`
		Collection<Condition> dbAndConditions = toConditions(tableQuery);
		// Typically happens on `COUNT(*)`
		dbAndConditions.removeIf(c -> c instanceof True);

		SelectConnectByStep<Record> selectFromWhere;
		if (dbAndConditions.isEmpty()) {
			selectFromWhere = selectFrom;
		} else {
			selectFromWhere = selectFrom.where(dbAndConditions);
		}

		// `GROUP BY ...`
		Collection<GroupField> groupFields = makeGroupingFields(tableQuery);
		SelectHavingStep<Record> selectFromWhereGroupBy = selectFromWhere.groupBy(groupFields);

		// `ORDER BY ...`
		ResultQuery<Record> resultQuery;
		if (tableQuery.getTopClause().isPresent()) {
			Collection<? extends OrderField<?>> optOrderFields = getOptionalOrders(tableQuery);

			resultQuery = selectFromWhereGroupBy.orderBy(optOrderFields).limit(tableQuery.getTopClause().getLimit());
		} else {
			resultQuery = selectFromWhereGroupBy;
		}

		// DO NOT EXPLAIN as it is already done in AdhocJooqTableWrapper.openDbStream
		// if (dbQuery.isExplain() || dbQuery.isDebug()) {
		// log.info("[EXPLAIN] SQL to db: `{}`", resultQuery.getSQL(ParamType.INLINED));
		// }

		return resultQuery;
	}

	protected Collection<Condition> toConditions(TableQuery dbQuery) {
		Collection<Condition> dbAndConditions = new ArrayList<>();

		dbAndConditions.add(oneMeasureIsNotNull(dbQuery.getAggregators()));

		IAdhocFilter filter = dbQuery.getFilter();
		if (!filter.isMatchAll()) {
			dbAndConditions.add(toCondition(filter));
		}

		return dbAndConditions;
	}

	protected List<SelectFieldOrAsterisk> makeSelectedFields(TableQuery tableQuery) {
		List<SelectFieldOrAsterisk> selectedFields = new ArrayList<>();
		tableQuery.getAggregators()
				.stream()
				.distinct()
				.map(a -> toSqlAggregatedColumn(a))
				// EmptyAggregation leads to no SQL aggregation
				.filter(a -> a != null)
				.forEach(a -> selectedFields.add(a));

		tableQuery.getGroupBy().getNameToColumn().values().forEach(column -> {
			Field<Object> field = columnAsField(column);
			selectedFields.add(field);
		});

		if (selectedFields.isEmpty()) {
			// Typically happens on EmptyAggregation
			// We force one field to prevent JooQ querying automatically for `*`
			selectedFields.add(DSL.val(1));
		}

		return selectedFields;
	}

	@Override
	public AggregatedRecordFields makeSelectedColumns(TableQuery tableQuery) {
		return TableQuery.makeSelectedColumns(tableQuery);
	}

	/**
	 * Most usages are not groupBy.
	 * 
	 * @param column
	 * @return a {@link Field} mapping to given column.
	 */
	protected Field<Object> columnAsField(IAdhocColumn column) {
		return columnAsField(column, false);
	}

	protected Field<Object> columnAsField(IAdhocColumn column, boolean isGroupBy) {
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
		if (ICountMeasuresConstants.ASTERISK.equals(columnName)) {
			// Typically on `COUNT(*)`
			return true;
		} else if (columnName.indexOf('"') >= 0) {
			// Typically on `max("v") FILTER("color" in ('red'))`
			return true;
		} else {
			return false;
		}
	}

	protected Name name(String name) {
		// BEWARE it is unclear what's the proper way of quoting
		// `unquotedName` is useful to handle input columns referencing (e.g. joined) tables as in
		// `tableName.columnName`, while `quoted` would treat `tableName.columnName` as a columnName

		if (isExpression(name)) {
			return DSL.unquotedName(name);
		}

		// Split by dot as we expect a regular use of `.` to reference joined tables
		String[] namesWithoutDot = name.split("\\.");

		return DSL.quotedName(namesWithoutDot);
	}

	protected Collection<GroupField> makeGroupingFields(TableQuery dbQuery) {
		List<GroupField> groupedFields = new ArrayList<>();

		dbQuery.getGroupBy().getNameToColumn().values().forEach(column -> {
			Field<Object> field = columnAsField(column, true);
			groupedFields.add(field);
		});

		return groupedFields;
	}

	private List<? extends OrderField<?>> getOptionalOrders(TableQuery dbQuery) {
		AdhocTopClause topClause = dbQuery.getTopClause();
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

	protected SelectFieldOrAsterisk toSqlAggregatedColumn(Aggregator a) {
		String aggregationKey = a.getAggregationKey();
		String columnName = a.getColumnName();
		Name namedColumn = name(columnName);

		if (ExpressionAggregation.KEY.equals(aggregationKey)) {
			// Do not call `name` to make sure it is not qualified
			if (namedColumn.qualified()) {
				// This is useful to ensure default expression detector is valid
				log.warn("Ambiguous expression: {}", columnName);
			}
			return DSL.field(DSL.unquotedName(columnName)).as(a.getName());
		} else {
			AggregateFunction<?> sqlAggFunction;
			if (SumAggregation.KEY.equals(aggregationKey)) {
				// DSL.field(namedColumn, Number.class); fails with `Type class java.lang.Number is not supported in
				// dialect
				// DEFAULT`
				Field<Double> field =
						DSL.field(namedColumn, DefaultDataType.getDataType(dslContext.dialect(), Double.class));

				sqlAggFunction = DSL.sum(field);
			} else if (MaxAggregation.KEY.equals(aggregationKey)) {
				Field<?> field = DSL.field(namedColumn);
				sqlAggFunction = DSL.max(field);
			} else if (MinAggregation.KEY.equals(aggregationKey)) {
				Field<?> field = DSL.field(namedColumn);
				sqlAggFunction = DSL.min(field);
			} else if (CountAggregation.KEY.equals(aggregationKey)) {
				Field<?> field = DSL.field(namedColumn);
				sqlAggFunction = DSL.count(field);
			} else if (EmptyAggregation.isEmpty(aggregationKey)) {
				return null;
			} else {
				throw new UnsupportedOperationException(
						"SQL does not support aggregationKey=%s".formatted(aggregationKey));
			}
			return sqlAggFunction.as(a.getName());
		}
	}

	protected Condition oneMeasureIsNotNull(Set<Aggregator> aggregators) {
		// We're interested in a row if at least one measure is not null
		List<Condition> oneNotNullConditions = aggregators.stream()
				.filter(a -> !EmptyAggregation.isEmpty(a.getAggregationKey()))
				.map(Aggregator::getColumnName)
				.filter(columnName -> !isExpression(columnName))
				.map(c -> DSL.field(name(c)).isNotNull())
				.collect(Collectors.toList());

		if (oneNotNullConditions.isEmpty()) {
			// Typically happens when the only measure is `COUNT(*)`
			return DSL.trueCondition();
		}

		return DSL.or(oneNotNullConditions);
	}

	protected Condition toCondition(IAdhocFilter filter) {
		if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			return toCondition(columnFilter);
		} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			Set<IAdhocFilter> operands = andFilter.getOperands();
			// TODO Detect and report if multiple conditions hits the same column
			// It would be the symptom of conflicting transcoding
			List<Condition> conditions = operands.stream().map(this::toCondition).toList();

			return DSL.and(conditions);
		} else if (filter.isOr() && filter instanceof IOrFilter orFilter) {
			Set<IAdhocFilter> operands = orFilter.getOperands();
			List<Condition> conditions = operands.stream().map(this::toCondition).toList();
			return DSL.or(conditions);
		} else {
			throw new UnsupportedOperationException(
					"Not handled: %s".formatted(PepperLogHelper.getObjectAndClass(filter)));
		}
	}

	protected Condition toCondition(IColumnFilter columnFilter) {
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
		default -> throw new UnsupportedOperationException(
				"Not handled: %s".formatted(PepperLogHelper.getObjectAndClass(columnFilter)));
		}
		return condition;
	}

}
