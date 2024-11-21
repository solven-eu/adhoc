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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SelectConditionStep;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IAndFilter;
import eu.solven.adhoc.api.v1.filters.IColumnFilter;
import eu.solven.adhoc.api.v1.pojo.value.EqualsMatcher;
import eu.solven.adhoc.api.v1.pojo.value.IValueMatcher;
import eu.solven.adhoc.api.v1.pojo.value.InMatcher;
import eu.solven.adhoc.api.v1.pojo.value.LikeMatcher;
import eu.solven.adhoc.api.v1.pojo.value.NullMatcher;
import eu.solven.adhoc.query.DatabaseQuery;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Wraps a {@link Connection} and rely on JooQ to use it as database for {@link DatabaseQuery}.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Builder
@Slf4j
public class JooqSqlDatabase implements IAdhocDatabaseWrapper {
	@Builder.Default
	@NonNull
	@Getter
	final IAdhocDatabaseTranscoder transcoder = new IdentityTranscoder();

	@NonNull
	final Supplier<Connection> connectionSupplier;

	@NonNull
	final String tableName;

	@Override
	public Stream<Map<String, ?>> openDbStream(DatabaseQuery dbQuery) {
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

		SelectConditionStep<Record> sqlQuery =
				makeDsl().select(selectedFields).from(DSL.name(tableName)).where(dbConditions);

		if (dbQuery.isExplain()) {
			log.info("[EXPLAIN] SQL to db: `{}`", sqlQuery.getSQL(ParamType.INLINED));
		}

		Stream<Map<String, ?>> dbStream = sqlQuery.stream().map(Record::intoMap);

		return dbStream.filter(row -> {
			// We could have a fallback, to filter manually when it is not doable by the DB (or we do not know how to
			// build the proper filter)
			return true;
		}).map(row -> {
			// In case of manual filters, we may have to hide some some columns, needed by the manual filter, but
			// unexpected by the output stream
			return transcode(row);
		});
	}

	private Collection<SelectFieldOrAsterisk> makeSelectedFields(DatabaseQuery dbQuery) {
		Collection<SelectFieldOrAsterisk> selectedFields = new ArrayList<>();
		dbQuery.getAggregators()
				.stream()
				.map(Aggregator::getColumnName)
				.map(transcoder::underlying)
				.distinct()
				.forEach(c -> selectedFields.add(DSL.field(c)));

		dbQuery.getGroupBy()
				.getGroupedByColumns()
				.stream()
				.map(transcoder::underlying)
				.distinct()
				.forEach(c -> selectedFields.add(DSL.field(c)));
		return selectedFields;
	}

	protected Map<String, ?> transcode(Map<String, ?> underlyingMap) {
		return AdhocTranscodingHelper.transcode(transcoder, underlyingMap);
	}

	protected Condition oneMeasureIsNotNull(Set<Aggregator> aggregators) {
		// We're interested in a row if at least one measure is not null
		List<Condition> oneNotNullConditions = aggregators.stream()
				.map(Aggregator::getColumnName)
				.map(c -> DSL.field(c).isNotNull())
				.collect(Collectors.toList());

		return DSL.or(oneNotNullConditions);
	}

	public DSLContext makeDsl() {
		return DSL.using(connectionSupplier.get());
	}

	protected Condition toCondition(IAdhocFilter filter) {
		if (filter.isColumnMatcher() && filter instanceof IColumnFilter columnFilter) {
			IValueMatcher valueMatcher = columnFilter.getValueMatcher();
			String column = transcoder.underlying(columnFilter.getColumn());

			Condition condition;
			final Field<Object> field = DSL.field(column);
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
