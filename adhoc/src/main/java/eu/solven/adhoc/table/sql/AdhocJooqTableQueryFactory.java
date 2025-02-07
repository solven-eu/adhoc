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
import java.util.Collections;
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
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultDataType;

import eu.solven.adhoc.column.IAdhocColumn;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregator;
import eu.solven.adhoc.measure.sum.CountAggregator;
import eu.solven.adhoc.measure.sum.SumAggregator;
import eu.solven.adhoc.measure.transformers.Aggregator;
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
	@NonNull
	final IAdhocTableTranscoder transcoder;

	// BEWARE Should we enable table as SQL or TableLike?
	@NonNull
	final TableLike<?> table;

	@NonNull
	DSLContext dslContext;

	@Override
	public ResultQuery<Record> prepareQuery(TableQuery dbQuery) {
		// `SELECT ...`
		Collection<SelectFieldOrAsterisk> selectedFields = makeSelectedFields(dbQuery);

		// `FROM ...`
		SelectJoinStep<Record> selectFrom = dslContext.select(selectedFields).from(table);

		// `WHERE ...`
		Collection<Condition> dbAndConditions = toConditions(dbQuery);
		// Typically happens on `COUNT(*)`
		dbAndConditions.removeIf(c -> c instanceof True);

		SelectConnectByStep<Record> selectFromWhere;
		if (dbAndConditions.isEmpty()) {
			selectFromWhere = selectFrom;
		} else {
			selectFromWhere = selectFrom.where(dbAndConditions);
		}

		// `GROUP BY ...`
		Collection<GroupField> groupFields = makeGroupingFields(dbQuery);
		SelectHavingStep<Record> selectFromWhereGroupBy;
		if (groupFields.isEmpty()) {
			selectFromWhereGroupBy = selectFromWhere;
		} else {
			selectFromWhereGroupBy = selectFromWhere.groupBy(groupFields);
		}

		// `ORDER BY ...`
		ResultQuery<Record> resultQuery;
		if (dbQuery.getTopClause().isPresent()) {
			Collection<? extends OrderField<?>> optOrderFields = getOptionalOrders(dbQuery);

			resultQuery = selectFromWhereGroupBy.orderBy(optOrderFields).limit(dbQuery.getTopClause().getLimit());
		} else {
			resultQuery = selectFromWhereGroupBy;
		}

		if (dbQuery.isExplain() || dbQuery.isDebug()) {
			log.info("[EXPLAIN] SQL to db: `{}`", resultQuery.getSQL(ParamType.INLINED));
		}

		return resultQuery;
	}

	protected Collection<Condition> toConditions(TableQuery dbQuery) {
		Collection<Condition> dbAndConditions = new ArrayList<>();

		dbAndConditions.add(oneMeasureIsNotNull(dbQuery.getAggregators()));

		IAdhocFilter filter = dbQuery.getFilter();
		if (!filter.isMatchAll()) {
			if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
				andFilter.getOperands().stream().map(this::toCondition).forEach(dbAndConditions::add);
			} else {
				dbAndConditions.add(toCondition(filter));
			}
		}

		return dbAndConditions;
	}

	protected Collection<SelectFieldOrAsterisk> makeSelectedFields(TableQuery dbQuery) {
		Collection<SelectFieldOrAsterisk> selectedFields = new ArrayList<>();
		dbQuery.getAggregators().stream().distinct().forEach(a -> selectedFields.add(toSqlAggregatedColumn(a)));

		dbQuery.getGroupBy().getNameToColumn().values().forEach(column -> {
			Field<Object> field = columnAsField(column);
			selectedFields.add(field);
		});
		return selectedFields;
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
		String columnName = column.getColumn();
		String transcodedName = transcoder.underlying(columnName);
		Field<Object> field;

		if (column instanceof IHasSqlExpression hasSql) {
			// TODO How could we transcode column referred by the SQL?
			// Should we add named columns from transcoder?
			String sql = hasSql.getSql();
			field = DSL.field(sql).as(columnName);
		} else {
			Field<Object> unaliasedField = DSL.field(name(transcodedName));

			if (isGroupBy || transcodedName.equals(columnName)) {
				// GroupBy: refer to the underlying column, to prevent ambiguities
				// https://github.com/duckdb/duckdb/issues/16097
				// https://github.com/jOOQ/jOOQ/issues/17980
				field = unaliasedField;
			} else {
				field = unaliasedField.as(name(columnName));
			}
		}
		return field;
	}

	protected Name name(String name) {
		// BEWARE it is unclear what's the proper way of quoting
		// `unquotedName` is useful to handle input columns referencing (e.g. joined) tables as in
		// `tableName.columnName`, while `quoted` would treat `tableName.columnName` as a columnName

		if (CountAggregator.ASTERISK.equals(name)) {
			// Typically on `COUNT(*)`
			return DSL.unquotedName(name);
		}

		// Split by dot as we expect a regular use of `.` to reference joined tables
		String[] namesWithoutDot = name.split("\\.");

		return DSL.quotedName(namesWithoutDot);
	}

	protected Collection<GroupField> makeGroupingFields(TableQuery dbQuery) {
		Collection<Field<Object>> groupedFields = new ArrayList<>();

		dbQuery.getGroupBy().getNameToColumn().values().forEach(column -> {
			Field<Object> field = columnAsField(column, true);
			groupedFields.add(field);
		});

		if (groupedFields.isEmpty()) {
			return Collections.emptyList();
		}

		return Collections.singleton(DSL.groupingSets(groupedFields));
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
		String columnName = transcoder.underlying(a.getColumnName());
		Name namedColumn = name(columnName);

		AggregateFunction<?> sqlAggFunction;
		if (SumAggregator.KEY.equals(aggregationKey)) {
			// DSL.field(namedColumn, Number.class); fails with `Type class java.lang.Number is not supported in dialect
			// DEFAULT`
			Field<Double> field =
					DSL.field(namedColumn, DefaultDataType.getDataType(dslContext.dialect(), Double.class));

			sqlAggFunction = DSL.sum(field);
		} else if (MaxAggregator.KEY.equals(aggregationKey)) {
			Field<?> field = DSL.field(namedColumn);
			sqlAggFunction = DSL.max(field);
		} else if (CountAggregator.KEY.equals(aggregationKey)) {
			Field<?> field = DSL.field(namedColumn);
			sqlAggFunction = DSL.count(field);
		} else {
			throw new UnsupportedOperationException("SQL does not support aggregationKey=%s".formatted(aggregationKey));
		}
		return sqlAggFunction.as(a.getName());
	}

	protected Condition oneMeasureIsNotNull(Set<Aggregator> aggregators) {
		// We're interested in a row if at least one measure is not null
		List<Condition> oneNotNullConditions = aggregators.stream()
				.map(Aggregator::getColumnName)
				.filter(columnName -> !CountAggregator.ASTERISK.equals(columnName))
				.map(transcoder::underlying)
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
			List<IAdhocFilter> operands = andFilter.getOperands();
			List<Condition> conditions = operands.stream().map(this::toCondition).collect(Collectors.toList());
			return DSL.and(conditions);
		} else if (filter.isOr() && filter instanceof IOrFilter orFilter) {
			List<IAdhocFilter> operands = orFilter.getOperands();
			List<Condition> conditions = operands.stream().map(this::toCondition).collect(Collectors.toList());
			return DSL.or(conditions);
		} else {
			throw new UnsupportedOperationException(
					"Not handled: %s".formatted(PepperLogHelper.getObjectAndClass(filter)));
		}
	}

	protected Condition toCondition(IColumnFilter columnFilter) {
		IValueMatcher valueMatcher = columnFilter.getValueMatcher();
		String column = transcoder.underlying(columnFilter.getColumn());

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
