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
package eu.solven.adhoc.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.GroupField;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultDataType;

import eu.solven.adhoc.aggregations.max.MaxAggregator;
import eu.solven.adhoc.aggregations.sum.SumAggregator;
import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IAndFilter;
import eu.solven.adhoc.api.v1.filters.IColumnFilter;
import eu.solven.adhoc.api.v1.pojo.value.ComparingMatcher;
import eu.solven.adhoc.api.v1.pojo.value.EqualsMatcher;
import eu.solven.adhoc.api.v1.pojo.value.IValueMatcher;
import eu.solven.adhoc.api.v1.pojo.value.InMatcher;
import eu.solven.adhoc.api.v1.pojo.value.LikeMatcher;
import eu.solven.adhoc.api.v1.pojo.value.NullMatcher;
import eu.solven.adhoc.query.DatabaseQuery;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This is especially important to make sure all calls to {@link IAdhocDatabaseTranscoder} relies on a
 * {@link TranscodingContext}
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Builder
@Slf4j
public class AdhocJooqSqlDatabaseStreamOpener {
	@NonNull
	final IAdhocDatabaseTranscoder transcoder;

	@NonNull
	final String tableName;

	@NonNull
	DSLContext dslContext;

	public ResultQuery<Record> prepareQuery(DatabaseQuery dbQuery) {
		Collection<Condition> dbConditions = new ArrayList<>();

		dbConditions.add(oneMeasureIsNotNull(dbQuery.getAggregators()));

		IAdhocFilter filter = dbQuery.getFilter();
		if (!filter.isMatchAll()) {
			if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
				andFilter.getAnd().stream().map(this::toCondition).forEach(dbConditions::add);
			} else {
				dbConditions.add(toCondition(filter));
			}
		}

		Collection<SelectFieldOrAsterisk> selectedFields = makeSelectedFields(dbQuery);

		Collection<GroupField> groupFields = new ArrayList<>();

		{
			dbQuery.getGroupBy().getGroupedByColumns().stream().map(transcoder::underlying).distinct().forEach(c -> {
				Field<?> field = DSL.field(DSL.name(c));
				groupFields.add(DSL.groupingSets(field));
			});
		}

		return dslContext.select(selectedFields).from(DSL.name(tableName)).where(dbConditions).groupBy(groupFields);
	}

	protected Collection<SelectFieldOrAsterisk> makeSelectedFields(DatabaseQuery dbQuery) {
		Collection<SelectFieldOrAsterisk> selectedFields = new ArrayList<>();
		dbQuery.getAggregators().stream().distinct().forEach(a -> selectedFields.add(toSqlAggregatedColumn(a)));

		dbQuery.getGroupBy()
				.getGroupedByColumns()
				.stream()
				.map(transcoder::underlying)
				.distinct()
				.forEach(c -> selectedFields.add(DSL.field(DSL.name(c))));
		return selectedFields;
	}

	protected SelectFieldOrAsterisk toSqlAggregatedColumn(Aggregator a) {
		String aggregationKey = a.getAggregationKey();
		String columnName = transcoder.underlying(a.getColumnName());
		Name namedColumn = DSL.name(columnName);

		if (SumAggregator.KEY.equals(aggregationKey)) {
			Field<Double> field =
					DSL.field(namedColumn, DefaultDataType.getDataType(dslContext.dialect(), Double.class));
			return DSL.sum(field).as(DSL.name(a.getName()));
		} else if (MaxAggregator.KEY.equals(aggregationKey)) {
			Field<?> field = DSL.field(namedColumn);
			return DSL.max(field).as(DSL.name(a.getName()));
		} else {
			throw new UnsupportedOperationException("SQL does not support aggregationKey=%s".formatted(aggregationKey));
		}
	}

	protected Condition oneMeasureIsNotNull(Set<Aggregator> aggregators) {
		// We're interested in a row if at least one measure is not null
		List<Condition> oneNotNullConditions = aggregators.stream()
				.map(Aggregator::getColumnName)
				.map(c -> transcoder.underlying(c))
				.map(c -> DSL.field(DSL.name(c)).isNotNull())
				.collect(Collectors.toList());

		return DSL.or(oneNotNullConditions);
	}

	protected Condition toCondition(IAdhocFilter filter) {
		if (filter.isColumnMatcher() && filter instanceof IColumnFilter columnFilter) {
			IValueMatcher valueMatcher = columnFilter.getValueMatcher();
			String column = transcoder.underlying(columnFilter.getColumn());

			Condition condition;
			final Field<Object> field = DSL.field(DSL.name(column));
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
					"Not handled: %s".formatted(PepperLogHelper.getObjectAndClass(filter)));
			}
			return condition;
		} else {
			throw new UnsupportedOperationException(
					"Not handled: %s".formatted(PepperLogHelper.getObjectAndClass(filter)));
		}
	}
}
